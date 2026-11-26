use claude_codex_server::workspaces::shell_environment_overrides;
use serde_json::{Value, json};
use std::process::Command;

fn parse(
    value: Value,
) -> Result<
    Option<std::collections::BTreeMap<String, String>>,
    claude_codex_server::protocol::RpcError,
> {
    shell_environment_overrides(value.as_object().unwrap())
}

#[test]
fn desktop_flattened_policy_preserves_overlay_and_validates_unsupported_semantics() {
    let env = parse(json!({"shell_environment_policy.inherit":"all","shell_environment_policy.set":{"WORKTREE_NAME":"feature"},"shell_environment_policy.exclude":[],"shell_environment_policy.include_only":[],"shell_environment_policy.ignore_default_excludes":true,"shell_environment_policy.experimental_use_profile":false})).unwrap().unwrap();
    assert_eq!(env["WORKTREE_NAME"], "feature");
    for policy in [
        json!({"inherit":"none"}),
        json!({"exclude":["SECRET"]}),
        json!({"include_only":["PATH"]}),
        json!({"ignore_default_excludes":false}),
        json!({"experimental_use_profile":true}),
        json!({"set":{"BAD=NAME":"x"}}),
        json!({"set":{"OK":"nul\u{0000}"}}),
    ] {
        assert!(parse(json!({"shell_environment_policy":policy})).is_err());
    }
    assert!(parse(json!({})).unwrap().is_none());
    assert!(
        parse(json!({"shell_environment_policy.set":{}}))
            .unwrap()
            .unwrap()
            .is_empty()
    );
    let env = parse(json!({"shell_environment_policy":{"set":{"KEY":"nested"}},"shell_environment_policy.set.KEY":"flat"})).unwrap().unwrap();
    assert_eq!(env["KEY"], "flat");
}

#[test]
fn desktop_created_git_worktree_remains_owned_by_desktop_and_accepts_environment_overlay() {
    let fixture = tempfile::tempdir().unwrap();
    let repo = fixture.path().join("repo");
    let worktree = fixture.path().join("managed-worktree");
    std::fs::create_dir(&repo).unwrap();
    let git = |cwd: &std::path::Path, args: &[&str]| {
        let output = Command::new("git")
            .current_dir(cwd)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "Fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@example.test")
            .env("GIT_COMMITTER_NAME", "Fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@example.test")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&repo, &["init", "--initial-branch=main"]);
    git(&repo, &["commit", "--allow-empty", "-m", "fixture"]);
    git(
        &repo,
        &[
            "worktree",
            "add",
            "--detach",
            worktree.to_str().unwrap(),
            "HEAD",
        ],
    );
    let env = parse(json!({"shell_environment_policy.inherit":"all","shell_environment_policy.set":{"WORKSPACE_LABEL":"desktop checkout"},"shell_environment_policy.exclude":[]})).unwrap().unwrap();
    let output = Command::new("sh")
        .current_dir(&worktree)
        .envs(env)
        .args([
            "-c",
            "printf '%s\\n' \"$WORKSPACE_LABEL\"; git rev-parse --show-toplevel",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines[0], "desktop checkout");
    assert_eq!(
        std::path::Path::new(lines[1]).canonicalize().unwrap(),
        worktree.canonicalize().unwrap()
    );
    assert!(worktree.join(".git").is_file());
}
