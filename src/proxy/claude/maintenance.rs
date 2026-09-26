use super::*;
use crate::{claude::maintenance::parse_usage, usage::UsageSnapshot};
use anyhow::{bail, ensure};
use http_body_util::Limited;
use std::{path::Path, process::Stdio};
use tokio::io::AsyncReadExt;

impl App {
    pub(crate) async fn refresh_claude_credentials_at(&self, now: u64) {
        for (account, kind) in &self.config.accounts {
            let AccountConfig::ClaudeHome { path } = kind else {
                continue;
            };
            if self.claude.auth.needs_login(path).await {
                self.router.reauth_required(account).await;
                continue;
            }
            if self
                .router
                .routing_snapshot()
                .await
                .account_states
                .get(account)
                .is_some_and(|s| s.unavailable_reason.as_deref() == Some("login_in_progress"))
            {
                continue;
            }
            self.stats
                .refresh_accounts_checked
                .fetch_add(1, Ordering::Relaxed);
            let expired = auth::read(path).is_ok_and(|c| c.expires_at <= now + 60);
            match self.claude.auth.resolve(path, None).await {
                Ok(_) => {
                    self.router.proactive_auth_ready(account).await;
                    if expired {
                        self.stats.refresh_successes.fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .refresh_last_success_unix
                            .store(now, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    self.stats.refresh_failures.fetch_add(1, Ordering::Relaxed);
                    if self.claude.auth.needs_login(path).await {
                        self.router.reauth_required(account).await;
                        self.stats
                            .refresh_reauth_required
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    pub(crate) async fn refresh_claude_usage_at(&self, now: u64, forced: bool) -> bool {
        let mut success = true;
        for (account, kind) in &self.config.accounts {
            let AccountConfig::ClaudeHome { path } = kind else {
                continue;
            };
            if self.claude.auth.needs_login(path).await {
                self.router.reauth_required(account).await;
                continue;
            }
            if self.shutting_down.load(Ordering::Acquire) {
                break;
            }
            if self
                .router
                .routing_snapshot()
                .await
                .account_states
                .get(account)
                .is_some_and(|s| s.unavailable_reason.as_deref() == Some("login_in_progress"))
            {
                continue;
            }
            if self
                .claude
                .usage_backoff
                .lock()
                .await
                .get(account)
                // An explicit refresh skips the normal cadence but never provider throttling.
                .is_some_and(|(until, delay)| *until > now && (!forced || *delay > 0))
            {
                continue;
            }
            self.stats
                .usage_fetch_accounts_checked
                .fetch_add(1, Ordering::Relaxed);
            match self.fetch_claude_usage(account, path, now).await {
                Ok((snapshot, credential)) => {
                    self.stats
                        .usage_fetch_successes
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .usage_fetch_last_success_unix
                        .store(now, Ordering::Relaxed);
                    if let Err(error) = self
                        .warm_claude_account(account, path, &snapshot, &credential)
                        .await
                    {
                        tracing::warn!(account,%error,"Claude window warming failed");
                    }
                }
                Err(error) => {
                    success = false;
                    self.stats
                        .usage_fetch_failures
                        .fetch_add(1, Ordering::Relaxed);
                    // Keep routine failures out of a tight scheduler loop. Explicit provider
                    // retry-after below can extend this per-account cooldown.
                    self.claude
                        .usage_backoff
                        .lock()
                        .await
                        .entry(account.clone())
                        .and_modify(|(until, _)| *until = (*until).max(now + 60))
                        .or_insert((now + 60, 0));
                    if self.claude.auth.needs_login(path).await {
                        self.router.reauth_required(account).await;
                    }
                    tracing::warn!(account, %error, "Claude usage refresh unavailable");
                }
            }
        }
        success
    }

    async fn fetch_claude_usage(
        &self,
        account: &str,
        home: &Path,
        now: u64,
    ) -> Result<(UsageSnapshot, auth::Credential)> {
        let mut credential = self.claude.auth.resolve(home, None).await?;
        for attempt in 0..2 {
            let request = Request::get(&self.claude.usage_url)
                .header(
                    "authorization",
                    format!("Bearer {}", credential.access_token),
                )
                .header("anthropic-beta", "oauth-2025-04-20")
                .header(
                    "user-agent",
                    concat!("comradex/", env!("CARGO_PKG_VERSION")),
                )
                .header("content-type", "application/json")
                .body(bytes_body(Bytes::new()))?;
            let (status, headers, bytes) = tokio::time::timeout(Duration::from_secs(8), async {
                let response = self.claude.client.request(request).await?;
                let (parts, body) = response.into_parts();
                let bytes = Limited::new(body, 1024 * 1024)
                    .collect()
                    .await
                    .map_err(|_| anyhow::anyhow!("invalid Claude usage response"))?
                    .to_bytes();
                Ok::<_, anyhow::Error>((parts.status, parts.headers, bytes))
            })
            .await
            .context("Claude usage timeout")??;
            if status == StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    credential = self
                        .claude
                        .auth
                        .resolve(home, Some(&credential.access_token))
                        .await?;
                    continue;
                }
                self.claude.auth.require_login(home).await;
                self.router.reauth_required(account).await;
            }
            if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE
            {
                let retry = headers
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| {
                        v.parse::<u64>().ok().or_else(|| {
                            chrono::DateTime::parse_from_rfc2822(v)
                                .ok()
                                .map(|at| (at.timestamp().max(0) as u64).saturating_sub(now))
                        })
                    });
                let mut backoff = self.claude.usage_backoff.lock().await;
                let previous = backoff.get(account).map_or(0, |(_, delay)| *delay);
                let delay = usage_throttle_delay(retry, previous);
                backoff.insert(account.into(), (now.saturating_add(delay), delay));
            }
            ensure!(
                status.is_success(),
                "Claude usage request rejected (HTTP {})",
                status.as_u16()
            );
            let snapshot = parse_usage(&bytes, now)?;
            self.router
                .observe_claude_usage_for_owner(account, snapshot.clone(), &credential.owner())
                .await;
            self.router.proactive_auth_ready(account).await;
            // The shared usage loop reruns within a minute whenever another account fails.
            // Keep this account on the normal cadence regardless.
            self.claude.usage_backoff.lock().await.insert(
                account.into(),
                (now + crate::usage::REFRESH_INTERVAL_SECONDS, 0),
            );
            return Ok((snapshot, credential));
        }
        bail!("Claude usage authentication failed")
    }

    async fn warm_claude_account(
        &self,
        account: &str,
        home: &Path,
        snapshot: &UsageSnapshot,
        credential: &auth::Credential,
    ) -> Result<()> {
        let key = blake3::hash(
            format!(
                "claude:{}:{}",
                credential.organization_uuid, credential.account_uuid
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string();
        let mut guard = self.claude.activation.lock().await;
        let ledger = guard
            .as_mut()
            .map_err(|_| anyhow::anyhow!("Claude activation ledger unavailable"))?;
        let now = auth::now() as i64;
        let Some(_cycle) = ledger.observe(&key, snapshot, now)? else {
            return Ok(());
        };
        if !self.config.proxy.auto_activate_claude_usage
            || self.shutting_down.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let Ok(_slot) = self.http_slots.try_acquire() else {
            return Ok(());
        };
        let state = self.router.routing_snapshot().await;
        if !state
            .account_states
            .get(account)
            .is_some_and(|s| s.available && s.inflight == 0)
        {
            return Ok(());
        }
        // Snapshot one real identity without racing credential import. Native warming
        // receives only its access token, so it cannot become a second refresh owner.
        let Some(_lock) = crate::auth_lock::HomeAuthLock::try_acquire(home)? else {
            return Ok(());
        };
        let current = auth::read(home)?;
        if current.owner() != credential.owner() || current.expires_at <= auth::now() + 60 {
            return Ok(());
        }
        ledger.reserve(&key, now)?;
        drop(_lock);
        self.router.begin(account).await;
        let _lease = DirectAccountLease::new(self.router.clone(), account.to_owned());
        run_native_warm(&current).await?;
        ledger.complete(&key, auth::now() as i64)?;
        tracing::info!(
            account,
            "Claude quota window warmed through native Claude Code"
        );
        Ok(())
    }
}

async fn run_native_warm(credential: &auth::Credential) -> Result<()> {
    run_native_warm_with(credential, auth::clean_command()?).await
}

async fn run_native_warm_with(
    credential: &auth::Credential,
    mut command: std::process::Command,
) -> Result<()> {
    let profile = tempfile::tempdir()?;
    let home = std::fs::canonicalize(profile.path())?;
    let metadata = serde_json::json!({"userID":credential.device_id,"oauthAccount":{"accountUuid":credential.account_uuid,"organizationUuid":credential.organization_uuid},"hasCompletedOnboarding":true});
    std::fs::write(
        profile.path().join(".claude.json"),
        serde_json::to_vec(&metadata)?,
    )?;
    command
        .args([
            "--print",
            "Reply only OK.",
            "--model",
            "haiku",
            "--tools",
            "",
            "--max-turns",
            "1",
            "--no-session-persistence",
            "--output-format",
            "json",
            "--setting-sources",
            "",
            "--strict-mcp-config",
            "--safe-mode",
        ])
        .current_dir(&home)
        .env("CLAUDE_CONFIG_DIR", &home)
        .env("CLAUDE_SECURESTORAGE_CONFIG_DIR", &home)
        .env("CLAUDE_CODE_OAUTH_TOKEN", &credential.access_token);
    let mut command = tokio::process::Command::from(command);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("launch native Claude warming")?;
    let stdout = child
        .stdout
        .take()
        .context("capture native Claude warming")?;
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut bytes = Vec::new();
        stdout.take(1024 * 1024 + 1).read_to_end(&mut bytes).await?;
        ensure!(
            bytes.len() <= 1024 * 1024,
            "native warming output too large"
        );
        let status = child.wait().await?;
        ensure!(status.success(), "native warming failed");
        let result: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid native warming result"))?;
        ensure!(
            result["type"] == "result"
                && result["subtype"] == "success"
                && result["is_error"] == false,
            "native warming did not complete"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("native Claude warming timed out")?
}

/// Anthropic's usage endpoint answers `retry-after: 0` while it keeps throttling. Poll no
/// faster than the normal cadence, and double the wait while throttling continues.
fn usage_throttle_delay(retry_after: Option<u64>, previous: u64) -> u64 {
    let escalated = if previous == 0 {
        crate::usage::REFRESH_INTERVAL_SECONDS
    } else {
        previous.saturating_mul(2).min(60 * 60)
    };
    retry_after.unwrap_or(0).max(escalated)
}

#[cfg(test)]
mod tests {
    #[test]
    fn usage_throttle_delay_honors_long_retry_after_and_caps_escalation() {
        use super::usage_throttle_delay;
        assert_eq!(usage_throttle_delay(Some(0), 0), 300);
        assert_eq!(usage_throttle_delay(None, 300), 600);
        assert_eq!(usage_throttle_delay(Some(0), 2400), 3600);
        assert_eq!(usage_throttle_delay(Some(0), 3600), 3600);
        assert_eq!(usage_throttle_delay(Some(7200), 0), 7200);
        assert_eq!(usage_throttle_delay(Some(7200), 7200), 7200);
    }
    use super::*;
    #[tokio::test]
    #[ignore = "rotates a real managed Claude grant and spends subscription quota; explicitly authorize before running"]
    async fn live_managed_claude_refresh_and_warming() {
        let home = std::env::var_os("COMRADEX_LIVE_CLAUDE_HOME")
            .expect("explicit managed Claude home required");
        let home = Path::new(&home);
        let before = auth::read(home).unwrap();
        let config = Config {
            proxy: Default::default(),
            listeners: Default::default(),
            pools: Default::default(),
            accounts: std::collections::BTreeMap::from([(
                "test".into(),
                AccountConfig::ClaudeHome {
                    path: home.to_owned(),
                },
            )]),
        };
        let refreshed = auth::Resolver::new(&config)
            .resolve(home, Some(&before.access_token))
            .await
            .unwrap();
        assert!(
            refreshed.owner() == before.owner() && refreshed.device_id == before.device_id,
            "refresh changed native identity"
        );
        assert!(
            refreshed.expires_at > auth::now() + 60,
            "refreshed credential is not usable"
        );
        let saved = auth::read(home).unwrap();
        assert!(
            saved.access_token == refreshed.access_token
                && saved.refresh_token == refreshed.refresh_token,
            "rotated grant was not persisted"
        );
        run_native_warm(&refreshed).await.unwrap();
    }
    #[tokio::test]
    async fn claude_warming_uses_an_isolated_native_invocation_without_a_refresh_grant() {
        let credential = auth::Credential {
            access_token: "sk-ant-oat01-synthetic".into(),
            refresh_token: "must-not-be-exported".into(),
            expires_at: auth::now() + 3600,
            account_uuid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".into(),
            organization_uuid: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb".into(),
            device_id: "a".repeat(64),
        };
        let mut command = std::process::Command::new("/bin/sh");
        command.args([
            "-c",
            r#"test "$CLAUDE_CODE_OAUTH_TOKEN" = sk-ant-oat01-synthetic || exit 1
test "$PWD" = "$CLAUDE_CONFIG_DIR" || exit 2
test "$CLAUDE_CONFIG_DIR" = "$CLAUDE_SECURESTORAGE_CONFIG_DIR" || exit 3
test -f .claude.json || exit 4
test ! -f .credentials.json || exit 5
test "$1" = --print || exit 6
test "$2" = 'Reply only OK.' || exit 7
printf '%s' '{"type":"result","subtype":"success","is_error":false}'
"#,
            "native-cli-test",
        ]);
        run_native_warm_with(&credential, command).await.unwrap();
        let mut failure = std::process::Command::new("/bin/sh");
        failure.args([
            "-c",
            "printf '%s' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":true}'",
            "native-cli-test",
        ]);
        assert!(run_native_warm_with(&credential, failure).await.is_err());
    }
}
