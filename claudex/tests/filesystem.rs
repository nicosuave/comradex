use base64::{Engine, engine::general_purpose::STANDARD};
use claude_codex_server::filesystem::dispatch;
use serde_json::json;

#[tokio::test]
async fn binary_files_copy_and_remove_without_following_source_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let data = [0, 255, 1, 2, 3];
    dispatch(
        "fs/writeFile",
        &json!({"path":source.join("file"),"dataBase64":STANDARD.encode(data)}),
    )
    .await
    .unwrap();
    let result = dispatch("fs/readFile", &json!({"path":source.join("file")}))
        .await
        .unwrap();
    assert_eq!(
        STANDARD
            .decode(result["dataBase64"].as_str().unwrap())
            .unwrap(),
        data
    );
    #[cfg(unix)]
    std::os::unix::fs::symlink("missing", source.join("link")).unwrap();
    let destination = root.path().join("destination");
    assert!(
        dispatch(
            "fs/copy",
            &json!({"sourcePath":source,"destinationPath":destination})
        )
        .await
        .is_err()
    );
    assert!(
        dispatch(
            "fs/copy",
            &json!({"sourcePath":source,"destinationPath":source.join("nested"),"recursive":true})
        )
        .await
        .is_err()
    );
    dispatch(
        "fs/copy",
        &json!({"sourcePath":source,"destinationPath":destination,"recursive":true}),
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(destination.join("file")).unwrap(), data);
    #[cfg(unix)]
    assert_eq!(
        std::fs::read_link(destination.join("link")).unwrap(),
        std::path::Path::new("missing")
    );
    assert!(
        dispatch("fs/remove", &json!({"path":destination,"recursive":false}))
            .await
            .is_err()
    );
    dispatch("fs/remove", &json!({"path":destination}))
        .await
        .unwrap();
    assert!(!destination.exists());
    assert!(source.exists());
    dispatch("fs/remove", &json!({"path":destination}))
        .await
        .unwrap();
    assert!(
        dispatch("fs/remove", &json!({"path":destination,"force":false}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn invalid_file_requests_do_not_modify_contents() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("file");
    std::fs::write(&path, "existing").unwrap();
    assert!(
        dispatch(
            "fs/writeFile",
            &json!({"path":path,"dataBase64":"!invalid"})
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "existing");
    assert!(
        dispatch("fs/readFile", &json!({"path":"relative"}))
            .await
            .is_err()
    );
    assert!(
        dispatch("fs/readFile", &json!({"path":root.path()}))
            .await
            .is_err()
    );
    assert!(
        dispatch(
            "fs/copy",
            &json!({"sourcePath":path,"destinationPath":path})
        )
        .await
        .is_err()
    );
    #[cfg(unix)]
    {
        let pipe = root.path().join("pipe");
        let name = std::ffi::CString::new(pipe.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(
            dispatch("fs/readFile", &json!({"path":pipe}))
                .await
                .is_err()
        );
    }
}
