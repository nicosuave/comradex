use std::{
    fs,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

const LABEL: &str = "com.nicosuave.comradex";
const READY_TIMEOUT: Duration = Duration::from_secs(8);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

pub fn install(config_path: &Path, state_dir: &Path) -> Result<PathBuf> {
    platform_check()?;
    let config_path = fs::canonicalize(config_path)
        .with_context(|| format!("resolve {}", config_path.display()))?;
    let config = crate::config::Config::load(&config_path)?;
    let listener_addresses: Vec<_> = config
        .listeners
        .values()
        .map(|listener| listener.address)
        .collect();
    if listener_addresses.iter().any(|address| address.port() == 0) {
        bail!("service installation requires fixed non-zero listener ports")
    }
    let executable = fs::canonicalize(std::env::current_exe()?)
        .context("resolve current Comradex executable")?;
    let plist_path = plist_path()?;
    let parent = plist_path
        .parent()
        .context("LaunchAgents path has no parent")?;
    fs::create_dir_all(parent)?;
    fs::create_dir_all(state_dir)?;
    let state_dir =
        fs::canonicalize(state_dir).with_context(|| format!("resolve {}", state_dir.display()))?;
    let stdout = state_dir.join("service.stdout.log");
    let stderr = state_dir.join("service.stderr.log");
    let working_directory = config_path.parent().unwrap_or(Path::new("/"));
    let service_nonce = format!("{:032x}", rand::random::<u128>());
    let plist = render_plist(
        &executable,
        &config_path,
        working_directory,
        &stdout,
        &stderr,
        &service_nonce,
    );
    let mut candidate = tempfile::NamedTempFile::new_in(parent)?;
    candidate.write_all(plist.as_bytes())?;
    candidate.as_file().sync_all()?;
    validate_plist(candidate.path())?;

    let previous = match fs::read(&plist_path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("read {}", plist_path.display())),
    };
    let domain = launchctl_domain();
    let was_loaded = is_loaded(&domain)?;
    if was_loaded && previous.is_none() {
        bail!(
            "LaunchAgent is loaded but {} is missing; refusing a replacement that cannot be rolled back",
            plist_path.display()
        )
    }
    replace_plist_transaction(
        &plist_path,
        candidate,
        previous.as_deref(),
        was_loaded,
        || stop_if_loaded(&domain),
        |path| {
            bootstrap(&domain, path)?;
            wait_until_ready(
                &domain,
                &listener_addresses,
                Some(&service_nonce),
                READY_TIMEOUT,
            )
        },
        |path| {
            bootstrap(&domain, path)?;
            wait_until_ready(&domain, &[], None, READY_TIMEOUT)
        },
    )?;
    Ok(plist_path)
}

fn render_plist(
    executable: &Path,
    config_path: &Path,
    working_directory: &Path,
    stdout: &Path,
    stderr: &Path,
    service_nonce: &str,
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{executable}</string>
    <string>--config</string><string>{config}</string>
    <string>serve</string>
  </array>
  <key>WorkingDirectory</key><string>{working_directory}</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>EnvironmentVariables</key>
  <dict><key>COMRADEX_SERVICE_NONCE</key><string>{service_nonce}</string></dict>
  <key>StandardOutPath</key><string>{stdout}</string>
  <key>StandardErrorPath</key><string>{stderr}</string>
</dict>
</plist>
"#,
        label = LABEL,
        executable = xml(executable),
        config = xml(config_path),
        working_directory = xml(working_directory),
        stdout = xml(stdout),
        stderr = xml(stderr),
        service_nonce = service_nonce,
    )
}

fn bootstrap(domain: &str, plist_path: &Path) -> Result<()> {
    let status = Command::new("launchctl")
        .args(["bootstrap", domain])
        .arg(plist_path)
        .status()
        .context("launch launchctl bootstrap")?;
    if !status.success() {
        bail!("launchctl bootstrap exited with {status}")
    }
    Ok(())
}

fn replace_plist_transaction<Stop, StartNew, StartPrevious>(
    plist_path: &Path,
    candidate: tempfile::NamedTempFile,
    previous: Option<&[u8]>,
    was_loaded: bool,
    mut stop: Stop,
    mut start_new: StartNew,
    mut start_previous: StartPrevious,
) -> Result<()>
where
    Stop: FnMut() -> Result<()>,
    StartNew: FnMut(&Path) -> Result<()>,
    StartPrevious: FnMut(&Path) -> Result<()>,
{
    if was_loaded {
        stop()?;
    }
    if let Err(error) = candidate.persist(plist_path) {
        let replace_error =
            anyhow::Error::new(error.error).context(format!("replace {}", plist_path.display()));
        if was_loaded && let Err(rollback_error) = start_previous(plist_path) {
            bail!("{replace_error:#}; restoring previous service failed: {rollback_error:#}")
        }
        return Err(replace_error);
    }
    let Err(start_error) = start_new(plist_path) else {
        return Ok(());
    };

    let rollback_result = stop()
        .and_then(|()| match previous {
            Some(bytes) => atomic_write(plist_path, bytes),
            None => match fs::remove_file(plist_path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            },
        })
        .and_then(|()| {
            if was_loaded {
                start_previous(plist_path)
            } else {
                Ok(())
            }
        });
    match rollback_result {
        Ok(()) => {
            Err(start_error.context("replacement LaunchAgent failed; previous service restored"))
        }
        Err(rollback_error) => {
            bail!(
                "replacement LaunchAgent failed: {start_error:#}; rollback failed: {rollback_error:#}"
            )
        }
    }
}

pub fn uninstall() -> Result<Option<PathBuf>> {
    platform_check()?;
    let plist_path = plist_path()?;
    let domain = launchctl_domain();
    if is_loaded(&domain)? {
        bootout(&domain)?;
    }
    if plist_path.exists() {
        fs::remove_file(&plist_path)?;
        Ok(Some(plist_path))
    } else {
        Ok(None)
    }
}

pub fn status() -> Result<bool> {
    platform_check()?;
    is_running(&launchctl_domain())
}

pub fn installed() -> Result<bool> {
    platform_check()?;
    Ok(plist_path()?.exists())
}

/// Restart the installed LaunchAgent so the daemon reloads its configuration.
/// The config path and readiness nonce come from the installed plist rather
/// than the CLI, and the configuration is validated before the daemon is
/// bounced so a broken edit fails here instead of taking the service down.
pub fn restart() -> Result<()> {
    platform_check()?;
    let plist_path = plist_path()?;
    let plist = match fs::read_to_string(&plist_path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!("service is not installed (run `comradex service install`)")
        }
        Err(error) => return Err(error).with_context(|| format!("read {}", plist_path.display())),
    };
    let config_path = plist_program_config(&plist)
        .with_context(|| format!("no --config argument recorded in {}", plist_path.display()))?;
    let config = crate::config::Config::load(&config_path)?;
    let listener_addresses: Vec<_> = config
        .listeners
        .values()
        .map(|listener| listener.address)
        .collect();
    let service_nonce = plist_string_after(&plist, "<key>COMRADEX_SERVICE_NONCE</key><string>");
    let domain = launchctl_domain();
    if is_loaded(&domain)? {
        kickstart(&domain)?;
    } else {
        bootstrap(&domain, &plist_path)?;
    }
    match &service_nonce {
        Some(nonce) => wait_until_ready(&domain, &listener_addresses, Some(nonce), READY_TIMEOUT),
        None => wait_until_ready(&domain, &[], None, READY_TIMEOUT),
    }
}

fn plist_program_config(plist: &str) -> Option<PathBuf> {
    plist_string_after(plist, "<string>--config</string><string>").map(PathBuf::from)
}

fn plist_string_after(plist: &str, marker: &str) -> Option<String> {
    let start = plist.find(marker)? + marker.len();
    let end = plist[start..].find("</string>")?;
    Some(xml_unescape(&plist[start..start + end]))
}

fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn kickstart(domain: &str) -> Result<()> {
    let status = Command::new("launchctl")
        .args(["kickstart", "-k", &format!("{domain}/{LABEL}")])
        .status()
        .context("launch launchctl kickstart")?;
    if !status.success() {
        bail!("launchctl kickstart exited with {status}")
    }
    Ok(())
}

fn is_loaded(domain: &str) -> Result<bool> {
    Ok(launchctl_print(domain)?.is_some())
}

fn is_running(domain: &str) -> Result<bool> {
    Ok(launchctl_print(domain)?.is_some_and(|output| launchctl_output_is_running(&output)))
}

fn launchctl_print(domain: &str) -> Result<Option<String>> {
    let output = Command::new("launchctl")
        .args(["print", &format!("{domain}/{LABEL}")])
        .stderr(Stdio::null())
        .output()
        .context("launch launchctl print")?;
    if !output.status.success() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&output.stdout).into_owned()))
}

fn launchctl_output_is_running(output: &str) -> bool {
    let running = output.lines().any(|line| line.trim() == "state = running");
    let has_pid = output.lines().any(|line| {
        line.trim()
            .strip_prefix("pid = ")
            .and_then(|pid| pid.parse::<u32>().ok())
            .is_some_and(|pid| pid > 0)
    });
    running && has_pid
}

fn wait_until_ready(
    domain: &str,
    listeners: &[SocketAddr],
    service_nonce: Option<&str>,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if is_running(domain)? && listeners_ready(listeners, service_nonce) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let targets = listeners
                .iter()
                .filter(|address| address.port() != 0)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            if targets.is_empty() {
                bail!("LaunchAgent did not reach a running state within {timeout:?}")
            }
            bail!("LaunchAgent did not become ready on {targets} within {timeout:?}")
        }
        thread::sleep(READY_POLL_INTERVAL);
    }
}

fn listeners_ready(listeners: &[SocketAddr], service_nonce: Option<&str>) -> bool {
    listeners
        .iter()
        .all(|address| service_nonce.is_some_and(|nonce| listener_ready(*address, nonce)))
}

fn listener_ready(address: SocketAddr, service_nonce: &str) -> bool {
    let address = probe_address(address);
    let Ok(mut stream) = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(CONNECT_TIMEOUT));
    let _ = stream.set_write_timeout(Some(CONNECT_TIMEOUT));
    let request = format!(
        "GET /__comradex_health/{service_nonce} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = Vec::new();
    if stream.take(8 * 1024).read_to_end(&mut response).is_err() {
        return false;
    }
    let response = String::from_utf8_lossy(&response);
    response.starts_with("HTTP/1.1 200") && response.contains("\"status\":\"ok\"")
}

fn probe_address(mut address: SocketAddr) -> SocketAddr {
    if address.ip().is_unspecified() {
        address.set_ip(match address.ip() {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        });
    }
    address
}

fn bootout(domain: &str) -> Result<()> {
    let status = Command::new("launchctl")
        .args(["bootout", &format!("{domain}/{LABEL}")])
        .status()
        .context("launch launchctl bootout")?;
    if !status.success() {
        bail!("launchctl bootout exited with {status}")
    }
    Ok(())
}

fn stop_if_loaded(domain: &str) -> Result<()> {
    if is_loaded(domain)? {
        bootout(domain)?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn launchctl_domain() -> String {
    format!("gui/{}", unsafe { libc::geteuid() })
}

#[cfg(not(target_os = "macos"))]
fn launchctl_domain() -> String {
    String::new()
}

fn plist_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home)
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist")))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn validate_plist(path: &Path) -> Result<()> {
    let status = Command::new("/usr/bin/plutil")
        .args(["-lint", "--"])
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("launch plutil")?;
    if !status.success() {
        bail!("generated LaunchAgent plist is invalid")
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn validate_plist(_path: &Path) -> Result<()> {
    bail!("plist validation is available on macOS only")
}

fn xml(path: &Path) -> String {
    path.to_string_lossy()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(target_os = "macos")]
fn platform_check() -> Result<()> {
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn platform_check() -> Result<()> {
    bail!("service management currently supports macOS LaunchAgents only")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn xml_escapes_paths() {
        assert_eq!(xml(Path::new("a&<b>\"c'")), "a&amp;&lt;b&gt;&quot;c&apos;");
    }

    #[test]
    fn restart_reads_config_path_and_nonce_from_rendered_plist() {
        let config = Path::new("/tmp/config<comradex>&'\".toml");
        let plist = render_plist(
            Path::new("/usr/local/bin/comradex"),
            config,
            Path::new("/tmp"),
            Path::new("/tmp/out.log"),
            Path::new("/tmp/err.log"),
            "nonce123",
        );
        assert_eq!(plist_program_config(&plist).unwrap(), config);
        assert_eq!(
            plist_string_after(&plist, "<key>COMRADEX_SERVICE_NONCE</key><string>").unwrap(),
            "nonce123"
        );
    }

    #[test]
    fn failed_replacement_restores_previous_plist_and_job() {
        let dir = tempfile::tempdir().unwrap();
        let plist_path = dir.path().join("service.plist");
        fs::write(&plist_path, b"old").unwrap();
        let mut candidate = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        candidate.write_all(b"new").unwrap();
        candidate.as_file().sync_all().unwrap();
        let stops = Cell::new(0);
        let new_starts = Cell::new(0);
        let previous_starts = Cell::new(0);

        let result = replace_plist_transaction(
            &plist_path,
            candidate,
            Some(b"old"),
            true,
            || {
                stops.set(stops.get() + 1);
                Ok(())
            },
            |path| {
                new_starts.set(new_starts.get() + 1);
                if fs::read(path)? == b"new" {
                    bail!("simulated bootstrap failure")
                }
                Ok(())
            },
            |path| {
                previous_starts.set(previous_starts.get() + 1);
                assert_eq!(fs::read(path)?, b"old");
                Ok(())
            },
        );

        assert!(result.is_err());
        assert_eq!(fs::read(&plist_path).unwrap(), b"old");
        assert_eq!(stops.get(), 2);
        assert_eq!(new_starts.get(), 1);
        assert_eq!(previous_starts.get(), 1);
    }

    #[test]
    fn launchctl_running_state_requires_a_pid() {
        assert!(launchctl_output_is_running("state = running\n\tpid = 42\n"));
        assert!(!launchctl_output_is_running("state = running\n"));
        assert!(!launchctl_output_is_running(
            "state = waiting\n\tpid = 42\n"
        ));
    }

    #[test]
    fn listener_probe_handles_wildcard_addresses() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let wildcard: SocketAddr = format!("0.0.0.0:{}", listener.local_addr().unwrap().port())
            .parse()
            .unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let length = stream.read(&mut request).unwrap();
            assert!(
                String::from_utf8_lossy(&request[..length])
                    .contains("/__comradex_health/test-nonce")
            );
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}",
                )
                .unwrap();
        });
        assert!(listeners_ready(&[wildcard], Some("test-nonce")));
        server.join().unwrap();
    }

    #[test]
    fn listener_probe_rejects_an_unrelated_service() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\nConnection: close\r\n\r\nnot found",
                )
                .unwrap();
        });
        assert!(!listeners_ready(&[address], Some("test-nonce")));
        server.join().unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn rendered_plist_passes_system_validation() {
        let dir = tempfile::tempdir().unwrap();
        let plist = render_plist(
            Path::new("/tmp/comradex&binary"),
            Path::new("/tmp/config<comradex>.toml"),
            Path::new("/tmp"),
            Path::new("/tmp/stdout.log"),
            Path::new("/tmp/stderr.log"),
            "test-nonce",
        );
        let mut file = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        file.write_all(plist.as_bytes()).unwrap();
        file.as_file().sync_all().unwrap();
        validate_plist(file.path()).unwrap();
    }
}
