//! Persistent, account-scoped Chrome user data. No debugging port or cookie access is needed.
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;

const MIN_CHROME_MAJOR: u32 = 115;
const RELEASES: &str = "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json";
pub const CODEX_DEVICE_URL: &str = "https://auth.openai.com/codex/device";

pub fn provider_url(claude: bool) -> &'static str {
    if claude {
        "https://claude.ai/login"
    } else {
        "https://chatgpt.com/auth/login"
    }
}

fn root(config: &Path) -> Result<PathBuf> {
    Ok(fs::canonicalize(config)?
        .parent()
        .context("configuration has no parent")?
        .join("browsers"))
}

pub fn profile_path(config: &Path, name: &str) -> Result<PathBuf> {
    // Hashing supports manually configured names without interpreting them as
    // paths, and isolates case-distinct names on case-insensitive filesystems.
    Ok(root(config)?
        .join("profiles")
        .join(blake3::hash(name.as_bytes()).to_hex().as_str()))
}

pub fn has_profile(config: &Path, name: &str) -> Result<bool> {
    Ok(profile_path(config, name)?.is_dir())
}

/// Refuse to delete cookies while Chrome is using them, or follow links outside
/// the storage owned by this configuration.
pub fn purgeable_profile(config: &Path, name: &str) -> Result<Option<PathBuf>> {
    let root = root(config)?;
    let profile = profile_path(config, name)?;
    for path in [&root, &root.join("profiles"), &profile] {
        match fs::symlink_metadata(path) {
            Ok(meta) => ensure!(
                meta.is_dir() && !meta.file_type().is_symlink(),
                "browser directory must be an ordinary directory: {}",
                path.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
    for marker in ["SingletonLock", "SingletonSocket"] {
        ensure!(
            fs::symlink_metadata(profile.join(marker))
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
            "close the browser for {name} before removing it with --purge (profile: {})",
            profile.display()
        );
    }
    Ok(Some(profile))
}

pub struct AccountBrowser {
    executable: PathBuf,
    profile: PathBuf,
}

impl AccountBrowser {
    pub fn prepare(config: &Path, name: &str) -> Result<Self> {
        ensure_display()?;
        let root = root(config)?;
        private_dir(&root)?;
        let executable = resolve_browser(&root)?;
        let profile = profile_path(config, name)?;
        private_dir(profile.parent().expect("profile parent"))?;
        private_dir(&profile)?;
        println!("browser profile for {name}: {}", profile.display());
        Ok(Self {
            executable,
            profile,
        })
    }

    pub fn open(&self, url: &str) -> Result<()> {
        open(&self.executable, &self.profile, url)
    }

    /// Claude executes BROWSER as a program, not a shell command with arguments.
    /// Keep the private launcher alive until the official login process exits.
    pub fn launcher(&self) -> Result<tempfile::TempDir> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("browser");
        let executable = std::env::current_exe()?;
        let script = format!(
            "#!/bin/sh\nexec {} browser-open --executable {} --profile {} -- \"$@\"\n",
            shell_quote(&executable)?,
            shell_quote(&self.executable)?,
            shell_quote(&self.profile)?
        );
        fs::write(&path, script)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        }
        Ok(directory)
    }
}

fn shell_quote(path: &Path) -> Result<String> {
    Ok(format!(
        "'{}'",
        path.to_str()
            .context("browser path must be UTF-8")?
            .replace('\'', "'\\''")
    ))
}

fn ensure_display() -> Result<()> {
    #[cfg(target_os = "linux")]
    ensure!(
        std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some(),
        "account browsers need a graphical session; on a headless host, pass --no-browser to account add or account login"
    );
    Ok(())
}

fn private_dir(path: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            !metadata.file_type().is_symlink(),
            "browser directory must not be a symlink: {}",
            path.display()
        );
    }
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn browser_command(executable: &Path, profile: &Path, url: &str) -> Command {
    let mut command = Command::new(executable);
    command
        .arg(format!("--user-data-dir={}", profile.display()))
        .args([
            "--no-first-run",
            "--no-default-browser-check",
            "--new-window",
            url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// Also used by the short-lived BROWSER launcher. Chrome keeps the profile window
/// alive after the CLI exits; a second invocation hands the URL to that instance.
pub fn open(executable: &Path, profile: &Path, url: &str) -> Result<()> {
    ensure!(
        url.starts_with("https://"),
        "account browser requires an HTTPS URL"
    );
    ensure!(
        executable.is_absolute() && profile.is_absolute(),
        "browser paths must be absolute"
    );
    let mut child = browser_command(executable, profile, url)
        .spawn()
        .context("launch account browser")?;
    std::thread::sleep(Duration::from_millis(300));
    if let Some(status) = child.try_wait()? {
        ensure!(
            status.success(),
            "Chrome exited with {status}; check its installation and graphical session"
        );
    } else {
        std::thread::spawn(move || {
            let _ = child.wait();
        });
    }
    Ok(())
}

fn chrome_major(version: &str) -> Option<u32> {
    if !["Google Chrome ", "Google Chrome for Testing ", "Chromium "]
        .iter()
        .any(|prefix| version.starts_with(prefix))
    {
        return None;
    }
    version.split_whitespace().find_map(|part| {
        let major = part.split_once('.')?.0.parse().ok()?;
        Some(major)
    })
}

fn supported(path: &Path) -> bool {
    Command::new(path)
        .arg("--version")
        .output()
        .is_ok_and(|output| {
            output.status.success()
                && chrome_major(&String::from_utf8_lossy(&output.stdout))
                    .is_some_and(|major| major >= MIN_CHROME_MAJOR)
        })
}

fn installed_candidates() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    #[cfg(target_os = "macos")]
    {
        let suffix = "Google Chrome.app/Contents/MacOS/Google Chrome";
        paths.push(PathBuf::from("/Applications").join(suffix));
        if let Some(home) = std::env::var_os("HOME") {
            paths.push(PathBuf::from(home).join("Applications").join(suffix));
        }
    }
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            for name in [
                "google-chrome",
                "google-chrome-stable",
                "chromium",
                "chromium-browser",
            ] {
                paths.push(dir.join(name));
            }
        }
    }
    paths
}

fn resolve_browser(root: &Path) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("COMRADEX_BROWSER") {
        let path = PathBuf::from(path);
        ensure!(
            path.is_absolute() && supported(&path),
            "COMRADEX_BROWSER must be an absolute path to Chrome/Chromium {MIN_CHROME_MAJOR} or newer"
        );
        return Ok(path);
    }
    if let Some(path) = installed_candidates()
        .into_iter()
        .find(|path| path.is_file() && supported(path))
    {
        return Ok(fs::canonicalize(path)?);
    }
    download_browser(root)
}

#[derive(Deserialize)]
struct Releases {
    channels: Channels,
}
#[derive(Deserialize)]
struct Channels {
    #[serde(rename = "Stable")]
    stable: Release,
}
#[derive(Deserialize)]
struct Release {
    version: String,
    downloads: Downloads,
}
#[derive(Deserialize)]
struct Downloads {
    chrome: Vec<Download>,
}
#[derive(Deserialize)]
struct Download {
    platform: String,
    url: String,
}

fn platform() -> Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("mac-arm64"),
        ("macos", "x86_64") => Ok("mac-x64"),
        ("linux", "x86_64") => Ok("linux64"),
        _ => bail!(
            "Google does not publish Chrome for Testing for this platform; install Chromium {MIN_CHROME_MAJOR}+ and set COMRADEX_BROWSER to its executable"
        ),
    }
}

fn downloaded_executable(directory: &Path, platform: &str) -> PathBuf {
    let directory = directory.join(format!("chrome-{platform}"));
    if platform.starts_with("mac-") {
        directory.join("Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing")
    } else {
        directory.join("chrome")
    }
}

fn release_download(releases: Releases, platform: &str) -> Result<(String, String)> {
    let release = releases.channels.stable;
    ensure!(
        release.version.split('.').count() == 4
            && release
                .version
                .split('.')
                .all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit())),
        "invalid Chrome release version"
    );
    let expected = format!(
        "https://storage.googleapis.com/chrome-for-testing-public/{}/{platform}/chrome-{platform}.zip",
        release.version
    );
    ensure!(
        release
            .downloads
            .chrome
            .iter()
            .any(|d| d.platform == platform && d.url == expected),
        "official Chrome download unavailable for {platform}"
    );
    Ok((release.version, expected))
}

fn curl(url: &str) -> Command {
    let mut command = Command::new("curl");
    command.args([
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--proto",
        "=https",
        "--proto-redir",
        "=https",
        "--connect-timeout",
        "20",
        "--max-time",
        "600",
        url,
    ]);
    command
}

fn download_browser(root: &Path) -> Result<PathBuf> {
    let platform = platform()?;
    let cache = root.join("chrome");
    private_dir(&cache)?;
    // Downloads publish by rename only after extraction and version verification.
    let _lock = crate::auth_lock::HomeAuthLock::acquire(&cache)?;
    let output = curl(RELEASES)
        .output()
        .context("fetch Chrome releases (curl is required)")?;
    ensure!(
        output.status.success(),
        "could not fetch stable Chrome release; install Chrome or set COMRADEX_BROWSER"
    );
    let (version, url) = release_download(
        serde_json::from_slice(&output.stdout).context("read Chrome releases")?,
        platform,
    )?;
    let destination = cache.join(&version);
    let executable = downloaded_executable(&destination, platform);
    if supported(&executable) {
        return Ok(executable);
    }
    ensure!(
        !destination.exists(),
        "cached Chrome is unusable; remove {} and retry",
        destination.display()
    );
    eprintln!("No supported Chrome found. Downloading Chrome for Testing {version} from Google...");
    let staging = tempfile::tempdir_in(&cache)?;
    let archive = staging.path().join("chrome.zip");
    ensure!(
        curl(&url)
            .arg("--output")
            .arg(&archive)
            .status()
            .context("download Chrome")?
            .success(),
        "Chrome download failed"
    );
    let unpacked = staging.path().join("unpacked");
    fs::create_dir(&unpacked)?;
    ensure!(
        Command::new("unzip")
            .arg("-q")
            .arg(&archive)
            .arg("-d")
            .arg(&unpacked)
            .status()
            .context("extract Chrome (unzip is required)")?
            .success(),
        "Chrome extraction failed"
    );
    ensure!(
        supported(&downloaded_executable(&unpacked, platform)),
        "downloaded Chrome cannot run on this system"
    );
    fs::rename(&unpacked, &destination)?;
    Ok(executable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "downloads and executes Google's current stable Chrome; requires network, curl and unzip"]
    fn download_current_stable_chrome() {
        let directory = tempfile::tempdir().unwrap();
        let executable = download_browser(directory.path()).unwrap();
        assert!(supported(&executable));
        assert_eq!(download_browser(directory.path()).unwrap(), executable);
    }

    #[test]
    fn profiles_are_stable_and_account_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("comradex.toml");
        fs::write(&config, "").unwrap();
        let grace = profile_path(&config, "Grace").unwrap();
        assert_ne!(grace, profile_path(&config, "grace").unwrap());
        assert_ne!(grace, profile_path(&config, "Ada").unwrap());
        assert_eq!(grace, profile_path(&config, "Grace").unwrap());
        assert_eq!(
            profile_path(&config, "../escape").unwrap().parent(),
            grace.parent()
        );
        private_dir(grace.parent().unwrap()).unwrap();
        private_dir(&grace).unwrap();
        fs::write(grace.join("cookie-fixture"), "retained").unwrap();
        private_dir(&grace).unwrap();
        assert_eq!(
            fs::read_to_string(grace.join("cookie-fixture")).unwrap(),
            "retained"
        );
        assert!(has_profile(&config, "Grace").unwrap());
    }

    #[test]
    fn versions_and_download_origins_are_checked() {
        assert_eq!(chrome_major("Google Chrome 140.0.1.2\n"), Some(140));
        assert_eq!(
            chrome_major("Google Chrome for Testing 140.0.1.2"),
            Some(140)
        );
        assert_eq!(chrome_major("Chromium 140.0.1.2"), Some(140));
        assert_eq!(chrome_major("unrelated 140.0.1.2"), None);
        let fixture = |url: &str, version: &str| {
            serde_json::from_value::<Releases>(serde_json::json!({"channels":{"Stable":{"version":version,"downloads":{"chrome":[{"platform":"mac-arm64","url":url}]}}}})).unwrap()
        };
        let url = "https://storage.googleapis.com/chrome-for-testing-public/140.0.1.2/mac-arm64/chrome-mac-arm64.zip";
        assert!(release_download(fixture(url, "140.0.1.2"), "mac-arm64").is_ok());
        assert!(
            release_download(
                fixture("https://example.com/chrome.zip", "140.0.1.2"),
                "mac-arm64"
            )
            .is_err()
        );
        assert!(release_download(fixture(url, "../escape"), "mac-arm64").is_err());
        assert!(release_download(fixture(url, "140.0.1.2"), "linux64").is_err());
    }

    #[test]
    fn browser_arguments_preserve_paths_and_urls_without_shell_interpretation() {
        let cmd = browser_command(
            Path::new("/Chrome App/chrome"),
            Path::new("/profiles/Grace's profile"),
            "https://claude.ai/oauth/authorize?a=1&b=2",
        );
        let args: Vec<_> = cmd.get_args().map(|s| s.to_str().unwrap()).collect();
        assert_eq!(args[0], "--user-data-dir=/profiles/Grace's profile");
        assert_eq!(
            args.last().unwrap(),
            &"https://claude.ai/oauth/authorize?a=1&b=2"
        );
        assert!(
            !args
                .iter()
                .any(|s| s.contains("remote-debugging") || s.contains("no-sandbox"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn profiles_are_private_and_reject_symlinks() {
        use std::os::unix::{fs::PermissionsExt, fs::symlink};
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join("profile");
        private_dir(&profile).unwrap();
        assert_eq!(
            fs::metadata(&profile).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let link = dir.path().join("linked");
        symlink(&profile, &link).unwrap();
        assert!(private_dir(&link).is_err());
    }

    #[test]
    fn launcher_paths_are_shell_quoted() {
        let path = Path::new("/tmp/Grace's $(touch never) browser");
        let output = Command::new("sh")
            .arg("-c")
            .arg(format!("printf '%s' {}", shell_quote(path).unwrap()))
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            path.to_str().unwrap()
        );
    }
}
