//! Filesystem operations requested by the desktop on its selected host.
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::protocol::{RpcError, RpcResult, required_str, supported_fields};

const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;

pub async fn dispatch(method: &str, params: &Value) -> RpcResult<Value> {
    let method = method.to_owned();
    let params = params.clone();
    tokio::task::spawn_blocking(move || execute(&method, &params))
        .await
        .map_err(RpcError::internal)?
}

fn absolute<'a>(params: &'a Value, name: &str) -> RpcResult<&'a Path> {
    let path = Path::new(required_str(params, name)?);
    if !path.is_absolute() {
        return Err(RpcError::invalid(format!("{name} must be absolute")));
    }
    Ok(path)
}

fn execute(method: &str, params: &Value) -> RpcResult<Value> {
    match method {
        "fs/readFile" => {
            supported_fields(params, &["path"])?;
            let path = absolute(params, "path")?;
            if !fs::metadata(path).map_err(RpcError::internal)?.is_file() {
                return Err(RpcError::invalid("fs/readFile requires a regular file"));
            }
            let file = fs::File::open(path).map_err(RpcError::internal)?;
            let metadata = file.metadata().map_err(RpcError::internal)?;
            if !metadata.is_file() || metadata.len() > MAX_FILE_BYTES {
                return Err(RpcError::invalid(
                    "fs/readFile requires a regular file of at most 16 MiB",
                ));
            }
            let mut data = Vec::new();
            file.take(MAX_FILE_BYTES + 1)
                .read_to_end(&mut data)
                .map_err(RpcError::internal)?;
            if data.len() as u64 > MAX_FILE_BYTES {
                return Err(RpcError::invalid(
                    "File grew beyond the 16 MiB response limit",
                ));
            }
            Ok(json!({"dataBase64":STANDARD.encode(data)}))
        }
        "fs/writeFile" => {
            supported_fields(params, &["path", "dataBase64"])?;
            let path = absolute(params, "path")?;
            let encoded = params["dataBase64"]
                .as_str()
                .ok_or_else(|| RpcError::invalid("dataBase64 must be a string"))?;
            let data = STANDARD
                .decode(encoded)
                .map_err(|_| RpcError::invalid("Invalid base64 file data"))?;
            if data.len() as u64 > MAX_FILE_BYTES {
                return Err(RpcError::invalid("fs/writeFile supports at most 16 MiB"));
            }
            if let Ok(metadata) = fs::metadata(path)
                && !metadata.is_file()
            {
                return Err(RpcError::invalid("fs/writeFile requires a regular file"));
            }
            fs::write(path, data).map_err(RpcError::internal)?;
            Ok(json!({}))
        }
        "fs/copy" => {
            supported_fields(params, &["sourcePath", "destinationPath", "recursive"])?;
            copy(
                absolute(params, "sourcePath")?,
                absolute(params, "destinationPath")?,
                params["recursive"] == true,
            )
            .map_err(RpcError::internal)?;
            Ok(json!({}))
        }
        "fs/remove" => {
            supported_fields(params, &["path", "recursive", "force"])?;
            let path = absolute(params, "path")?;
            let metadata = match fs::symlink_metadata(path) {
                Ok(metadata) => metadata,
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound && params["force"] != false =>
                {
                    return Ok(json!({}));
                }
                Err(error) => return Err(RpcError::internal(error)),
            };
            let result = if metadata.is_dir() {
                if params["recursive"] != false {
                    fs::remove_dir_all(path)
                } else {
                    fs::remove_dir(path)
                }
            } else {
                fs::remove_file(path)
            };
            result.map_err(RpcError::internal)?;
            Ok(json!({}))
        }
        _ => Err(RpcError::unsupported(method)),
    }
}

fn destination_path(path: &Path) -> std::io::Result<PathBuf> {
    // Resolve the nearest existing ancestor before checking for self-copies,
    // including destinations reached through a symlinked parent.
    let mut ancestor = path;
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        suffix.push(
            ancestor
                .file_name()
                .ok_or_else(|| std::io::Error::other("Invalid destination"))?
                .to_owned(),
        );
        ancestor = ancestor
            .parent()
            .ok_or_else(|| std::io::Error::other("Invalid destination"))?;
    }
    let mut resolved = ancestor.canonicalize()?;
    for component in suffix.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn copy(source: &Path, destination: &Path, recursive: bool) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.is_dir() {
        if !recursive {
            return Err(std::io::Error::other(
                "Directory copies require recursive=true",
            ));
        }
        if destination_path(destination)?.starts_with(source.canonicalize()?) {
            return Err(std::io::Error::other("Cannot copy a directory into itself"));
        }
    } else if metadata.is_file() && source.canonicalize()? == destination_path(destination)? {
        return Err(std::io::Error::other(
            "Source and destination are the same file",
        ));
    }
    let mut pending = vec![(source.to_owned(), destination.to_owned())];
    while let Some((source, destination)) = pending.pop() {
        let metadata = fs::symlink_metadata(&source)?;
        if metadata.file_type().is_symlink() {
            #[cfg(unix)]
            std::os::unix::fs::symlink(fs::read_link(&source)?, &destination)?;
            #[cfg(not(unix))]
            return Err(std::io::Error::other("Symlink copy requires Unix"));
        } else if metadata.is_dir() {
            fs::create_dir_all(&destination)?;
            for child in fs::read_dir(&source)? {
                let child = child?;
                pending.push((child.path(), destination.join(child.file_name())));
            }
        } else if metadata.is_file() {
            fs::copy(source, destination)?;
        } else {
            return Err(std::io::Error::other(
                "Only regular files, directories, and symlinks can be copied",
            ));
        }
    }
    Ok(())
}
