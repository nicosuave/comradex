#![cfg(unix)]

use std::{fs, path::Path, process::Command};

#[test]
fn release_archive_ships_one_executable_and_the_facade_notices() {
    let fixture = tempfile::tempdir().unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let archive = fixture.path().join("comradex.tar.gz");
    let output = Command::new("bash")
        .arg(root.join("scripts/package-archive.sh"))
        .arg(env!("CARGO_BIN_EXE_comradex"))
        .arg(&archive)
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);

    let output = Command::new("tar")
        .arg("-xzf")
        .arg(archive)
        .current_dir(fixture.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    for path in [
        "claudex/LICENSE",
        "claudex/NOTICE",
        "claudex/licenses/Codex-Apache-2.0.txt",
    ] {
        assert_eq!(
            fs::read(fixture.path().join(path)).unwrap(),
            fs::read(root.join(path)).unwrap()
        );
    }
    let mut root_entries: Vec<_> = fs::read_dir(fixture.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    root_entries.sort();
    assert_eq!(root_entries, ["claudex", "comradex", "comradex.tar.gz"]);
    for arguments in [&["--version"][..], &["desktop", "--version"][..]] {
        let version = Command::new(fixture.path().join("comradex"))
            .args(arguments)
            .output()
            .unwrap();
        assert!(version.status.success(), "{:?}", version);
        assert!(String::from_utf8_lossy(&version.stdout).contains(env!("CARGO_PKG_VERSION")));
    }
}
