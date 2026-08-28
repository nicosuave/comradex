use std::{sync::Arc, thread, time::Duration};

use anyhow::{Context, Result, ensure};
use hyper_rustls::HttpsConnector as RustlsHttpsConnector;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use rustls::{ClientConfig, RootCertStore, pki_types::CertificateDer};

// macOS can briefly return an empty trust store while a user's login session is
// still coming up. Keep this well below the service's 20-second readiness
// timeout while allowing the platform security services time to settle.
#[cfg(target_os = "macos")]
const TLS_ROOT_RETRY_DELAYS: &[Duration] = &[
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];
#[cfg(not(target_os = "macos"))]
const TLS_ROOT_RETRY_DELAYS: &[Duration] = &[];

/// Builds the same transport-default TLS shape used by Codex's reqwest 0.12 client.
///
/// In Codex 0.149.0 the normal Responses HTTP path uses `native-tls` without
/// reqwest's optional `native-tls-alpn` feature. Constructing the connector
/// explicitly keeps that no-ALPN ClientHello while allowing Hyper to continue
/// streaming request and response bodies without materializing them.
pub(crate) fn codex_http_connector() -> Result<HttpsConnector<HttpConnector>> {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    let tls = native_tls::TlsConnector::builder()
        .build()
        .context("build transport-default native TLS connector")?;
    Ok(HttpsConnector::from((http, tls.into())))
}

/// Builds the explicit Rustls transport used by Codex Responses WebSockets.
///
/// Codex uses AWS-LC, platform roots, and no ALPN for this HTTP/1.1 upgrade
/// path. `enable_http1` deliberately preserves the empty ALPN list.
pub(crate) fn codex_websocket_connector() -> Result<RustlsHttpsConnector<HttpConnector>> {
    let config = codex_websocket_tls_config()?;
    Ok(hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_or_http()
        .enable_http1()
        .build())
}

fn codex_websocket_tls_config() -> Result<ClientConfig> {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let builder = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .context("select Codex Rustls protocol versions")?;
    let roots = load_platform_root_store()?;
    Ok(builder.with_root_certificates(roots).with_no_client_auth())
}

fn load_platform_root_store() -> Result<RootCertStore> {
    load_platform_root_store_with(
        || {
            let native = rustls_native_certs::load_native_certs();
            (native.certs, native.errors.len())
        },
        thread::sleep,
        TLS_ROOT_RETRY_DELAYS,
    )
}

fn load_platform_root_store_with<Load, Sleep>(
    mut load: Load,
    mut sleep: Sleep,
    retry_delays: &[Duration],
) -> Result<RootCertStore>
where
    Load: FnMut() -> (Vec<CertificateDer<'static>>, usize),
    Sleep: FnMut(Duration),
{
    let mut attempt = 1;
    loop {
        let (certs, load_error_count) = load();
        match root_store_from_native_certs(certs, load_error_count) {
            Ok(roots) => return Ok(roots),
            Err(error) => match retry_delays.get(attempt - 1) {
                Some(delay) => {
                    sleep(*delay);
                    attempt += 1;
                }
                None => {
                    return Err(error).with_context(|| {
                        format!("load platform TLS roots after {attempt} attempts")
                    });
                }
            },
        }
    }
}

fn root_store_from_native_certs(
    certs: Vec<CertificateDer<'static>>,
    load_error_count: usize,
) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let (valid_count, invalid_count) = roots.add_parsable_certificates(certs);
    ensure!(
        valid_count > 0,
        "load platform TLS roots: no usable certificates ({load_error_count} load errors, {invalid_count} parse errors)"
    );
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use http_body_util::Empty;
    use hyper::{Request, body::Bytes};
    use hyper_util::{client::legacy::Client, rt::TokioExecutor};
    use md5::{Digest as _, Md5};
    use sha2::Sha256;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    use rustls::pki_types::CertificateDer;

    use super::{
        TLS_ROOT_RETRY_DELAYS, codex_http_connector, codex_websocket_connector,
        load_platform_root_store_with, root_store_from_native_certs,
    };

    #[test]
    fn websocket_root_store_rejects_no_native_certificates() {
        let error = root_store_from_native_certs(Vec::new(), 2).unwrap_err();
        assert_eq!(
            error.to_string(),
            "load platform TLS roots: no usable certificates (2 load errors, 0 parse errors)"
        );
    }

    #[test]
    fn websocket_root_store_rejects_only_unparsable_certificates() {
        let error = root_store_from_native_certs(
            vec![CertificateDer::from(vec![0xde, 0xad, 0xbe, 0xef])],
            0,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "load platform TLS roots: no usable certificates (0 load errors, 1 parse errors)"
        );
    }

    #[test]
    fn websocket_root_store_tolerates_bad_certificates_when_a_root_is_usable() {
        let native = rustls_native_certs::load_native_certs();
        let valid = native
            .certs
            .into_iter()
            .find(|cert| {
                let mut roots = rustls::RootCertStore::empty();
                roots.add(cert.clone()).is_ok()
            })
            .expect("test platform should provide at least one usable TLS root");
        let roots = root_store_from_native_certs(
            vec![CertificateDer::from(vec![0xde, 0xad, 0xbe, 0xef]), valid],
            1,
        )
        .unwrap();
        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn websocket_root_store_retries_until_platform_roots_are_available() {
        let native = rustls_native_certs::load_native_certs();
        let valid = native
            .certs
            .into_iter()
            .find(|cert| {
                let mut roots = rustls::RootCertStore::empty();
                roots.add(cert.clone()).is_ok()
            })
            .expect("test platform should provide at least one usable TLS root");
        let mut loads = 0;
        let mut sleeps = Vec::new();
        let roots = load_platform_root_store_with(
            || {
                loads += 1;
                if loads < 3 {
                    (Vec::new(), loads)
                } else {
                    (vec![valid.clone()], 0)
                }
            },
            |delay| sleeps.push(delay),
            &[
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(40),
            ],
        )
        .unwrap();

        assert_eq!(loads, 3);
        assert_eq!(
            sleeps,
            [Duration::from_millis(10), Duration::from_millis(20)]
        );
        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn websocket_root_store_fails_after_bounded_retries() {
        let mut loads = 0;
        let mut sleeps = Vec::new();
        let error = load_platform_root_store_with(
            || {
                loads += 1;
                (Vec::new(), loads)
            },
            |delay| sleeps.push(delay),
            &[Duration::from_millis(10), Duration::from_millis(20)],
        )
        .unwrap_err();

        assert_eq!(loads, 3);
        assert_eq!(
            sleeps,
            [Duration::from_millis(10), Duration::from_millis(20)]
        );
        assert_eq!(
            error.to_string(),
            "load platform TLS roots after 3 attempts"
        );
        assert_eq!(
            error.root_cause().to_string(),
            "load platform TLS roots: no usable certificates (3 load errors, 0 parse errors)"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn websocket_root_store_retry_budget_stays_below_service_ready_timeout() {
        let total: Duration = TLS_ROOT_RETRY_DELAYS.iter().sum();
        assert_eq!(total, Duration::from_millis(7_750));
        assert!(total < Duration::from_secs(20));
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ClientHelloProfile {
        legacy_version: u16,
        ciphers: Vec<u16>,
        extensions: Vec<u16>,
        supported_groups: Vec<u16>,
        point_formats: Vec<u8>,
        signature_algorithms: Vec<u16>,
        supported_versions: Vec<u16>,
        has_sni: bool,
        first_alpn: Option<Vec<u8>>,
    }

    impl ClientHelloProfile {
        fn ja3_raw(&self) -> String {
            format!(
                "{},{},{},{},{}",
                self.legacy_version,
                join_u16(&self.ciphers),
                join_u16(&self.extensions),
                join_u16(&self.supported_groups),
                self.point_formats
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join("-")
            )
        }

        fn ja3(&self) -> String {
            format!("{:x}", Md5::digest(self.ja3_raw().as_bytes()))
        }

        fn ja4_raw(&self) -> String {
            let version = self
                .supported_versions
                .iter()
                .copied()
                .max()
                .unwrap_or(self.legacy_version);
            let version = match version {
                0x0304 => "13",
                0x0303 => "12",
                0x0302 => "11",
                0x0301 => "10",
                _ => "00",
            };
            let alpn = self.first_alpn.as_deref().map_or_else(
                || "00".to_owned(),
                |value| match (value.first(), value.last()) {
                    (Some(first), Some(last)) => {
                        format!("{}{}", char::from(*first), char::from(*last))
                    }
                    _ => "00".to_owned(),
                },
            );
            let prefix = format!(
                "t{version}{}{:02}{:02}{alpn}",
                if self.has_sni { 'd' } else { 'i' },
                self.ciphers.len().min(99),
                self.extensions.len().min(99),
            );

            let mut ciphers = self.ciphers.clone();
            ciphers.sort_unstable();
            let cipher_hash = short_sha256(&join_hex_u16(&ciphers));

            let mut extensions = self
                .extensions
                .iter()
                .copied()
                .filter(|extension| *extension != 0x0000 && *extension != 0x0010)
                .collect::<Vec<_>>();
            extensions.sort_unstable();
            let extension_input = format!(
                "{}_{}",
                join_hex_u16(&extensions),
                join_hex_u16(&self.signature_algorithms)
            );
            format!("{prefix}_{cipher_hash}_{}", short_sha256(&extension_input))
        }
    }

    #[tokio::test]
    async fn native_http_clienthello_matches_codex_reqwest() {
        let (listener, port) = capture_listener().await;
        let capture = tokio::spawn(capture_client_hello(listener));
        let client = Client::builder(TokioExecutor::new()).build(codex_http_connector().unwrap());
        let request = Request::get(format!("https://localhost:{port}/"))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let _ = client.request(request).await;
        let ours = parse_client_hello(&capture.await.unwrap()).unwrap();

        let (listener, port) = capture_listener().await;
        let capture = tokio::spawn(capture_client_hello(listener));
        let codex = reqwest::Client::builder().no_proxy().build().unwrap();
        let _ = codex.get(format!("https://localhost:{port}/")).send().await;
        let reference = parse_client_hello(&capture.await.unwrap()).unwrap();

        assert_eq!(ours, reference);
        eprintln!("Codex-compatible JA3: {}", ours.ja3());
        eprintln!("Codex-compatible JA4: {}", ours.ja4_raw());
    }

    #[tokio::test]
    async fn websocket_clienthello_keeps_codex_rustls_profile() {
        let (listener, port) = capture_listener().await;
        let capture = tokio::spawn(capture_client_hello(listener));
        let client =
            Client::builder(TokioExecutor::new()).build(codex_websocket_connector().unwrap());
        let request = Request::get(format!("https://localhost:{port}/"))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let _ = client.request(request).await;
        let profile = parse_client_hello(&capture.await.unwrap()).unwrap();

        assert_eq!(profile.first_alpn, None);
        // Rustls deliberately permutes extension order, so JA3 changes between
        // connections. JA4 sorts the relevant inputs and remains stable.
        assert_eq!(profile.ja4_raw(), "t13d101000_61a7ad8aa9b6_f9531d972513");
        eprintln!("Codex WebSocket-compatible JA3: {}", profile.ja3());
        eprintln!("Codex WebSocket-compatible JA4: {}", profile.ja4_raw());
    }

    async fn capture_listener() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    async fn capture_client_hello(listener: TcpListener) -> Vec<u8> {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header[0], 22, "expected a TLS handshake record");
        let length = usize::from(u16::from_be_bytes([header[3], header[4]]));
        let mut record = vec![0u8; length];
        stream.read_exact(&mut record).await.unwrap();
        record
    }

    fn parse_client_hello(record: &[u8]) -> Result<ClientHelloProfile, String> {
        let mut cursor = Cursor::new(record);
        if cursor.u8()? != 1 {
            return Err("TLS handshake was not ClientHello".into());
        }
        let handshake_length = cursor.u24()?;
        let handshake = cursor.take(handshake_length)?;
        let mut hello = Cursor::new(handshake);
        let legacy_version = hello.u16()?;
        hello.take(32)?;
        let session_id_length = usize::from(hello.u8()?);
        hello.take(session_id_length)?;
        let cipher_length = usize::from(hello.u16()?);
        let ciphers = parse_u16_list(hello.take(cipher_length)?)
            .into_iter()
            .filter(|value| !is_grease(*value))
            .collect();
        let compression_length = usize::from(hello.u8()?);
        hello.take(compression_length)?;
        let extension_length = usize::from(hello.u16()?);
        let mut extension_cursor = Cursor::new(hello.take(extension_length)?);
        let mut extensions = Vec::new();
        let mut supported_groups = Vec::new();
        let mut point_formats = Vec::new();
        let mut signature_algorithms = Vec::new();
        let mut supported_versions = Vec::new();
        let mut has_sni = false;
        let mut first_alpn = None;
        let mut seen = BTreeSet::new();

        while !extension_cursor.is_empty() {
            let extension_type = extension_cursor.u16()?;
            let data_length = usize::from(extension_cursor.u16()?);
            let data = extension_cursor.take(data_length)?;
            if !is_grease(extension_type) {
                extensions.push(extension_type);
            }
            if !seen.insert(extension_type) {
                return Err(format!("duplicate TLS extension {extension_type:#06x}"));
            }
            match extension_type {
                0x0000 => has_sni = true,
                0x000a => supported_groups = parse_prefixed_u16_list(data)?,
                0x000b => point_formats = parse_prefixed_u8_list(data)?,
                0x000d => signature_algorithms = parse_prefixed_u16_list(data)?,
                0x0010 => first_alpn = parse_first_alpn(data)?,
                0x002b => supported_versions = parse_prefixed_u8_u16_list(data)?,
                _ => {}
            }
        }

        supported_groups.retain(|value| !is_grease(*value));
        supported_versions.retain(|value| !is_grease(*value));
        Ok(ClientHelloProfile {
            legacy_version,
            ciphers,
            extensions,
            supported_groups,
            point_formats,
            signature_algorithms,
            supported_versions,
            has_sni,
            first_alpn,
        })
    }

    fn parse_u16_list(data: &[u8]) -> Vec<u16> {
        data.as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
            .collect()
    }

    fn parse_prefixed_u16_list(data: &[u8]) -> Result<Vec<u16>, String> {
        let mut cursor = Cursor::new(data);
        let length = usize::from(cursor.u16()?);
        Ok(parse_u16_list(cursor.take(length)?))
    }

    fn parse_prefixed_u8_u16_list(data: &[u8]) -> Result<Vec<u16>, String> {
        let mut cursor = Cursor::new(data);
        let length = usize::from(cursor.u8()?);
        Ok(parse_u16_list(cursor.take(length)?))
    }

    fn parse_prefixed_u8_list(data: &[u8]) -> Result<Vec<u8>, String> {
        let mut cursor = Cursor::new(data);
        let length = usize::from(cursor.u8()?);
        Ok(cursor.take(length)?.to_vec())
    }

    fn parse_first_alpn(data: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let mut cursor = Cursor::new(data);
        let list_length = usize::from(cursor.u16()?);
        let mut list = Cursor::new(cursor.take(list_length)?);
        if list.is_empty() {
            return Ok(None);
        }
        let length = usize::from(list.u8()?);
        Ok(Some(list.take(length)?.to_vec()))
    }

    fn is_grease(value: u16) -> bool {
        let [high, low] = value.to_be_bytes();
        high == low && high & 0x0f == 0x0a
    }

    fn join_u16(values: &[u16]) -> String {
        values
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join("-")
    }

    fn join_hex_u16(values: &[u16]) -> String {
        values
            .iter()
            .map(|value| format!("{value:04x}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn short_sha256(value: &str) -> String {
        format!("{:x}", Sha256::digest(value.as_bytes()))[..12].to_owned()
    }

    struct Cursor<'a> {
        data: &'a [u8],
        offset: usize,
    }

    impl<'a> Cursor<'a> {
        fn new(data: &'a [u8]) -> Self {
            Self { data, offset: 0 }
        }

        fn is_empty(&self) -> bool {
            self.offset == self.data.len()
        }

        fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
            let end = self
                .offset
                .checked_add(length)
                .ok_or_else(|| "TLS length overflow".to_owned())?;
            let value = self
                .data
                .get(self.offset..end)
                .ok_or_else(|| "truncated TLS ClientHello".to_owned())?;
            self.offset = end;
            Ok(value)
        }

        fn u8(&mut self) -> Result<u8, String> {
            Ok(self.take(1)?[0])
        }

        fn u16(&mut self) -> Result<u16, String> {
            let bytes = self.take(2)?;
            Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
        }

        fn u24(&mut self) -> Result<usize, String> {
            let bytes = self.take(3)?;
            Ok(
                (usize::from(bytes[0]) << 16)
                    | (usize::from(bytes[1]) << 8)
                    | usize::from(bytes[2]),
            )
        }
    }
}
