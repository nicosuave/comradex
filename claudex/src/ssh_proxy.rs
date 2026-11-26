//! Git SSH transport through Claude's authenticated sandbox HTTP proxy.
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{Shutdown, TcpStream},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// The native env file is sourced inside each Bash command, after the sandbox
/// installs its per-command proxy variables. Never persist proxy credentials.
pub fn add_hook(settings: &mut Value, executable: &Path) -> Result<()> {
    let executable = executable.to_str().context("facade path must be UTF-8")?;
    if executable.contains(['\n', '\r', '%']) {
        bail!("facade path cannot contain newline or SSH token characters");
    }
    let hooks = settings
        .as_object_mut()
        .context("invalid settings")?
        .entry("hooks")
        .or_insert(json!({}))
        .as_object_mut()
        .context("invalid hooks")?
        .entry("SessionStart")
        .or_insert(json!([]))
        .as_array_mut()
        .context("invalid SessionStart hooks")?;
    // SSH performs its own token expansion and shell parsing of ProxyCommand.
    let proxy = format!("{} sandbox-proxy-connect %h %p", quote(executable));
    let command = format!(
        "ssh -o ControlMaster=no -o ControlPath=none -o {}",
        quote(&format!("ProxyCommand={proxy}"))
    );
    let script = format!(
        "\ncase \"${{SANDBOX_RUNTIME-}}:${{GIT_SSH_COMMAND-}}\" in\n  \"1:ssh -o ControlMaster=no -o ControlPath=none -o ProxyCommand='nc -X 5 -x localhost:\"*\" %h %p'\")\n    export GIT_SSH_COMMAND={}\n    ;;\nesac\n",
        quote(&command)
    );
    let hook = format!(
        "test -n \"$CLAUDE_ENV_FILE\" && printf %s {} >> \"$CLAUDE_ENV_FILE\"",
        quote(&script)
    );
    hooks.push(json!({"hooks":[{"type":"command","command":hook}]}));
    Ok(())
}

/// Connect only to the native loopback proxy. Its policy remains authoritative:
/// denied hosts, disabled network access and authentication failures never fall
/// back to a direct socket. Credentials stay in memory and out of diagnostics.
pub fn connect(host: &str, port: u16) -> Result<()> {
    if host.is_empty()
        || !host
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b".-_:[]".contains(&c))
        || port == 0
    {
        bail!("invalid SSH destination");
    }
    let address =
        std::env::var("CLOUDSDK_PROXY_ADDRESS").context("native proxy address missing")?;
    if address != "localhost" && address != "127.0.0.1" {
        bail!("native proxy must be loopback");
    }
    let proxy_port: u16 = std::env::var("CLOUDSDK_PROXY_PORT")
        .context("native proxy port missing")?
        .parse()
        .context("invalid native proxy port")?;
    let username =
        std::env::var("CLOUDSDK_PROXY_USERNAME").context("native proxy authentication missing")?;
    let password =
        std::env::var("CLOUDSDK_PROXY_PASSWORD").context("native proxy authentication missing")?;
    let mut stream = TcpStream::connect_timeout(
        &([127, 0, 0, 1], proxy_port).into(),
        Duration::from_secs(15),
    )?;
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    let target = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let auth = STANDARD.encode(format!("{username}:{password}"));
    write!(
        stream,
        "CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nProxy-Authorization: Basic {auth}\r\n\r\n"
    )?;
    let mut reader = BufReader::new(stream);
    let mut consumed = 0;
    let mut status = None;
    loop {
        let mut line = String::new();
        let n = reader
            .by_ref()
            .take(16385 - consumed)
            .read_line(&mut line)?;
        consumed += n as u64;
        if n == 0 || consumed > 16384 {
            bail!("invalid native proxy response");
        }
        if status.is_none() {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() < 2 || !fields[0].starts_with("HTTP/1.") {
                bail!("invalid native proxy status");
            }
            status = Some(fields[1].to_owned());
        }
        if line == "\r\n" {
            break;
        }
    }
    if status.as_deref() != Some("200") {
        bail!("native sandbox proxy refused SSH connection");
    }
    reader.get_ref().set_read_timeout(None)?;
    reader.get_ref().set_write_timeout(None)?;
    let mut writer = reader.get_ref().try_clone()?;
    // Detached stdin reader cannot hold shutdown open after the remote closes.
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut std::io::stdin().lock(), &mut writer);
        let _ = writer.shutdown(Shutdown::Write);
    });
    let mut output = std::io::stdout().lock();
    let mut buffer = [0; 16384];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        output.write_all(&buffer[..count])?;
        // SSH exchanges binary packets without newlines; stdout's line buffer
        // must not delay a packet until the remote waits for our response.
        output.flush()?;
    }
    Ok(())
}
