use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::mpsc,
};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::{
    codec::{FramedRead, FramedWrite, LinesCodec},
    sync::CancellationToken,
};

use crate::server::Event;

const MAX_FRAME: usize = 32 * 1024 * 1024;
const OUTPUT_QUEUE: usize = 256;
static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);

pub async fn serve(
    endpoint: &str,
    events: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<()> {
    if endpoint == "stdio://" {
        lines(tokio::io::stdin(), tokio::io::stdout(), events, shutdown).await;
        return Ok(());
    }
    if let Some(address) = endpoint.strip_prefix("ws://") {
        let address: SocketAddr = address
            .parse()
            .context("WebSocket endpoint must be ws://IP:PORT")?;
        if !address.ip().is_loopback() {
            bail!("Unauthenticated WebSocket transport must bind to a loopback IP");
        }
        let listener = TcpListener::bind(address).await?;
        eprintln!("Listening on ws://{}", listener.local_addr()?);
        loop {
            let (stream, _) = tokio::select! { _ = shutdown.cancelled() => break, result = listener.accept() => result? };
            let events = events.clone();
            let cancel = shutdown.child_token();
            tokio::spawn(websocket(stream, events, cancel));
        }
        return Ok(());
    }
    #[cfg(unix)]
    if let Some(path) = endpoint
        .strip_prefix("unix://")
        .or_else(|| endpoint.strip_prefix("unix-lines://"))
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            bail!("Unix socket path must be absolute");
        }
        remove_stale_socket(&path).await?;
        let listener = tokio::net::UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let metadata = std::fs::symlink_metadata(&path)?;
        struct SocketGuard(PathBuf, u64, u64);
        impl Drop for SocketGuard {
            fn drop(&mut self) {
                if std::fs::symlink_metadata(&self.0)
                    .is_ok_and(|m| m.ino() == self.1 && m.dev() == self.2)
                {
                    let _ = std::fs::remove_file(&self.0);
                }
            }
        }
        let _guard = SocketGuard(path.clone(), metadata.ino(), metadata.dev());
        eprintln!("Listening on {endpoint}");
        loop {
            let (stream, _) = tokio::select! { _ = shutdown.cancelled() => break, result = listener.accept() => result? };
            if endpoint.starts_with("unix-lines://") {
                let (reader, writer) = stream.into_split();
                tokio::spawn(lines(
                    reader,
                    writer,
                    events.clone(),
                    shutdown.child_token(),
                ));
            } else {
                tokio::spawn(websocket(stream, events.clone(), shutdown.child_token()));
            }
        }
        return Ok(());
    }
    bail!(
        "Unsupported transport; use stdio://, ws://127.0.0.1:PORT, unix:///absolute/path, or unix-lines:///absolute/path"
    )
}

async fn dispatch_line(id: u64, text: &str, events: &mpsc::Sender<Event>) -> Result<()> {
    if text.trim().is_empty() {
        return Ok(());
    }
    let event = match serde_json::from_str(text) {
        Ok(message) => Event::Input { id, message },
        Err(_) => Event::ParseError {
            id,
            message: "Invalid JSON".into(),
        },
    };
    events
        .send(event)
        .await
        .map_err(|_| anyhow::anyhow!("Server stopped"))
}

async fn lines<R, W>(reader: R, writer: W, events: mpsc::Sender<Event>, cancel: CancellationToken)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let id = NEXT_CLIENT.fetch_add(1, Ordering::Relaxed);
    let (output, mut receiver) = mpsc::channel::<Value>(OUTPUT_QUEUE);
    if events
        .send(Event::Connected {
            id,
            output,
            cancel: cancel.clone(),
        })
        .await
        .is_err()
    {
        return;
    }
    let mut reader = FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_FRAME));
    let mut writer = FramedWrite::new(writer, LinesCodec::new_with_max_length(MAX_FRAME));
    let writer_cancel = cancel.clone();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = writer_cancel.cancelled() => break,
                message = receiver.recv() => match message {
                    Some(value) => if writer.send(value.to_string()).await.is_err() { break; },
                    None => break,
                }
            }
        }
        writer_cancel.cancel();
    });
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            line = reader.next() => match line {
                Some(Ok(text)) => if dispatch_line(id, &text, &events).await.is_err() { break; },
                Some(Err(_)) => { let _ = events.send(Event::ParseError { id, message: "JSON frame exceeds limit or is not UTF-8".into() }).await; break; }
                None => break,
            }
        }
    }
    cancel.cancel();
    let _ = events.send(Event::Disconnected { id }).await;
    writer_task.abort();
    let _ = writer_task.await;
}

// TCP and Unix sockets use the same WebSocket handshake and message contract.
// Tungstenite's handshake callback requires its unboxed ErrorResponse type.
#[allow(clippy::result_large_err)]
async fn websocket<S>(stream: S, events: mpsc::Sender<Event>, cancel: CancellationToken)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use tokio_tungstenite::tungstenite::{
        handshake::server::{Request, Response},
        http::StatusCode,
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let handshake = accept_hdr_async_with_config(
        stream,
        |request: &Request, response: Response| {
            // This privileged endpoint serves native clients, not browser pages.
            // A page on localhost is no more trusted than any other website.
            if request.headers().contains_key("origin") {
                let mut error = tokio_tungstenite::tungstenite::http::Response::new(Some(
                    "Browser origins are not allowed".into(),
                ));
                *error.status_mut() = StatusCode::FORBIDDEN;
                return Err(error);
            }
            Ok(response)
        },
        Some(config),
    );
    let ws = tokio::select! {
        _ = cancel.cancelled() => return,
        result = tokio::time::timeout(std::time::Duration::from_secs(10), handshake) => match result {
            Ok(Ok(ws)) => ws,
            _ => return,
        }
    };
    let id = NEXT_CLIENT.fetch_add(1, Ordering::Relaxed);
    let (output, mut receiver) = mpsc::channel::<Value>(OUTPUT_QUEUE);
    if events
        .send(Event::Connected {
            id,
            output,
            cancel: cancel.clone(),
        })
        .await
        .is_err()
    {
        return;
    }
    let (mut sink, mut source) = ws.split();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            message = receiver.recv() => match message {
                Some(value) => if sink.send(Message::Text(value.to_string().into())).await.is_err() { break; },
                None => break,
            },
            frame = source.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    // Codex uses one JSON-RPC message per WebSocket text frame.
                    if dispatch_line(id, &text, &events).await.is_err() { break; }
                }
                Some(Ok(Message::Ping(data))) => if sink.send(Message::Pong(data)).await.is_err() { break; },
                Some(Ok(Message::Pong(_))) => {},
                Some(Ok(Message::Close(_))) => {
                    // Reading Close queues Tungstenite's reply. Flush it before
                    // dropping the connection so peers can finish the handshake.
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(1), sink.flush()
                    ).await;
                    break;
                }
                _ => break,
            }
        }
    }
    cancel.cancel();
    let _ = events.send(Event::Disconnected { id }).await;
}

#[cfg(unix)]
async fn remove_stale_socket(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_socket() || metadata.uid() != unsafe { libc::geteuid() } {
        bail!(
            "Refusing to replace a non-socket or foreign-owned path: {}",
            path.display()
        );
    }
    match tokio::net::UnixStream::connect(path).await {
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        _ => bail!(
            "Socket is active or cannot be confirmed stale: {}",
            path.display()
        ),
    }
    let current = std::fs::symlink_metadata(path)?;
    if current.dev() != metadata.dev() || current.ino() != metadata.ino() {
        bail!("Socket changed while checking it: {}", path.display());
    }
    std::fs::remove_file(path)?;
    Ok(())
}
