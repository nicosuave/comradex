#![cfg(all(unix, feature = "desktop-test-backends"))]

use std::{
    fs,
    net::TcpListener,
    os::unix::fs::{PermissionsExt, symlink},
    process::Stdio,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::process::Command;

const KEY: &str = "isolated-claude-desktop-acp-fixture-key";
const ALIAS: &str = "claude-sonnet-acp-fixture";

#[tokio::test]
async fn claude_desktop_can_discover_and_prompt_a_configured_acp_agent() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let root = directory.path();
    let gateway = TcpListener::bind("127.0.0.1:0").unwrap();
    let relay = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = gateway.local_addr().unwrap();
    let relay_address = relay.local_addr().unwrap();
    let tools = root.join("tools");
    fs::create_dir(&tools).unwrap();
    symlink(env!("CARGO_BIN_EXE_fake-acp"), tools.join("fixture-acp")).unwrap();
    let launcher = root.join("launch-acp");
    fs::write(&launcher, "#!/bin/sh\nexec fixture-acp \"$@\"\n").unwrap();
    fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
    let agent_program = json!(launcher);
    let agent_path = json!(tools);
    let pid_file = root.join("agent.pid");
    let agent_args = json!([
        format!("--store={}", root.join("agent").display()),
        format!("--pid-file={}", pid_file.display()),
    ]);
    let cwd = json!(root);
    let config = root.join("comradex.toml");
    std::fs::write(
        &config,
        format!(
            r#"
        [proxy]
        installation_secret = "fixture-installation-secret"
        affinity_key = "fixture-affinity-key-longer-than-32"
        [proxy.claude_desktop]
        address = "{address}"
        api_key = "{KEY}"
        [proxy.claude_desktop.acp.{ALIAS}]
        executable = {agent_program}
        args = {agent_args}
        cwd = {cwd}
        path = {agent_path}
        display_name = "Fixture ACP"
        model = "fake-b"
        [listeners.default]
        address = "{relay_address}"
        pool = "default"
        [pools.default]
        members = ["caller"]
        [accounts.caller]
        kind = "inbound"
    "#
        ),
    )
    .unwrap();
    drop((gateway, relay));
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_comradex"))
        .arg("--config")
        .arg(config)
        .arg("serve")
        .env_remove("COMRADEX_SERVICE_NONCE")
        .env("PATH", "/usr/bin:/bin")
        .current_dir(root)
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let origin = format!("http://{address}");
    let catalog = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(reply) = client
                .get(format!("{origin}/v1/models"))
                .bearer_auth(KEY)
                .send()
                .await
                && reply.status().is_success()
            {
                return serde_json::from_slice::<Value>(&reply.bytes().await.unwrap()).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("ACP-only gateway should listen");
    assert_eq!(catalog["data"][0]["id"], ALIAS);
    assert_eq!(catalog["data"][0]["display_name"], "Fixture ACP");
    assert!(
        !root.join("agent").exists(),
        "catalog lookup must not launch the agent"
    );

    let send = |messages: Value, stream: bool| {
        client
            .post(format!("{origin}/v1/messages?beta=true"))
            .header("x-api-key", KEY)
            .body(
                json!({"model":ALIAS,"max_tokens":1024,"messages":messages,"stream":stream})
                    .to_string(),
            )
            .send()
    };
    let user = |text| json!({"role":"user","content":text});
    let first = send(json!([user("hello")]), false).await.unwrap();
    assert!(first.status().is_success());
    let first: Value = serde_json::from_slice(&first.bytes().await.unwrap()).unwrap();
    assert_eq!(first["content"][0]["text"], "Echo: hello");
    assert_eq!(first["usage"]["cache_read_input_tokens"], 5);

    let second = send(
        json!([
            user("hello"),
            {"role":"assistant","content":first["content"]},
            {"role":"system","content":[{"type":"text","text":"Retain context."}]},
            user("history"),
        ]),
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        second.headers()[reqwest::header::CONTENT_TYPE],
        "text/event-stream"
    );
    let wire = second.text().await.unwrap();
    let events = stream_events(&wire);
    let text = stream_text(&events);
    assert!(text.contains("model=fake-b"), "{text}");
    assert_eq!(events.first().unwrap()["message"]["model"], ALIAS);
    assert_eq!(events.last().unwrap()["type"], "message_stop");

    let denied = send(json!([user("exec:fixture-only")]), true)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(denied.contains("gateway cannot grant"));
    assert!(stream_text(&stream_events(&denied)).starts_with("exec no "));
    assert!(!denied.contains("message_stop"));
    let failed = send(json!([user("error")]), false).await.unwrap();
    assert_eq!(failed.status(), reqwest::StatusCode::BAD_GATEWAY);
    let maxed = send(json!([user("max-tokens")]), false).await.unwrap();
    let maxed: Value = serde_json::from_slice(&maxed.bytes().await.unwrap()).unwrap();
    assert_eq!(maxed["stop_reason"], "max_tokens");

    let mut slow = send(json!([user("slow")]), true).await.unwrap();
    let beginning = slow.chunk().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&beginning).contains("message_start"));
    let pid: libc::pid_t = std::fs::read_to_string(pid_file).unwrap().parse().unwrap();
    assert_eq!(unsafe { libc::kill(pid, 0) }, 0, "fixture is running");
    drop(slow);
    tokio::time::timeout(Duration::from_secs(4), async {
        while unsafe { libc::kill(pid, 0) } == 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("closing the response should stop the ACP process");

    daemon.kill().await.unwrap();
}

fn stream_events(wire: &str) -> Vec<Value> {
    wire.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|text| serde_json::from_str(text).unwrap())
        .collect()
}

fn stream_text(events: &[Value]) -> String {
    events
        .iter()
        .filter_map(|event| event["delta"]["text"].as_str())
        .collect()
}
