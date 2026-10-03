---
name: comradex-self-hosted-dev
description: Develop and test Comradex without stranding agents whose inference runs through it. Use before replacing the running build, changing service lifecycle, or setting up test accounts.
---

# Developing Comradex through Comradex

Keep the serving relay available while developing its replacement. Any unavoidable
interruption must have recovery that completes without another model turn or tool
call through that relay. This skill does not authorize deployment or client
reconfiguration beyond the user's request.

## Identify the dependency

Check the execution hostname, the inference relay's host, and the installed
service's executable and config paths. Report only safe fields; never print
credential files, installation secrets, affinity keys, or secret listener URLs.

A different checkout, model/provider label, SSH host, or `--config` argument does
not establish an independent inference route. If the route is uncertain, treat
the affected relay as required by this session. A second Mac can use the same
relay.

On macOS, service commands target the singleton `com.nicosuave.comradex`
LaunchAgent. `service start` and `service restart` use its recorded configuration,
regardless of the CLI's `--config` argument.

## Test separately by default

- Run repository checks and process tests before live installation.
- For live protocol tests, run the candidate with `--config <test-config> serve`.
  Use separate config, state, control socket, logs, listener ports, and secrets.
- Point only test client processes at that daemon. Keep the working agent and
  normal client settings on the serving relay.
- Use inbound accounts when they cover the behavior. Managed-account tests need
  their own native login grant; do not clone or share rotating credentials.
- Do not use `account add`, `account login`, or `account connect` as isolated
  setup commands just because `--config` names a test file. They can restart or
  unload the installed LaunchAgent. Prepare the test config separately; use the
  test daemon's control protocol for managed login when needed.
- Test launchd behavior with a distinct dummy label and isolated paths.
- Clean up only the exact test processes, jobs, and files created by this task.

## Replace the serving build only when authorized

1. Build and validate the candidate before interrupting the service.
2. Stage it at an absolute, durable path outside disposable build/worktree output.
3. Retain the working executable and service definition for rollback.
4. Preserve existing account homes, routing ownership state, installation secret,
   and affinity key across the switch unless the requested change requires otherwise.
5. Establish recovery before the first disruptive command.

Use an already verified independent inference session, or an independently
supervised local job that owns the complete cutover and recovery. A detached
command must be shown to survive termination of its launching process. Test that
property against a dummy job, never by disconnecting the serving relay.

That job must validate its inputs, replace the service, observe readiness, recover
or restore the retained build on failure, and write a private result log without
needing further agent instructions. Give one owner the live transition; do not
launch competing installers or recovery jobs. Without working independent
recovery, finish the code and isolated tests and leave the live switch pending.

Do not run `service uninstall` to relocate or upgrade the binary. Invoke the
candidate's `--config <live-config> service install` directly: installation already
replaces the service definition. Uninstall removes both the running job and its
plist; `service start` cannot recover a deleted definition.

Read the lifecycle implementation before choosing the controller. Changed-plist
replacement and rollback must wait until launchd removes the old job after
`bootout`. Do not substitute a fixed sleep or assume a PR's existence means the
running controller contains its fix.

## Recover and verify

Inspect actual launchd state before retrying. A readiness timeout can mean a job
is still starting; it is not proof that the job was unloaded. Keep a valid starting
job intact while observing it within a bounded deadline. Use `service start` for
an installed but unloaded or stopped job; it leaves a running job uninterrupted.
Do not loop through uninstall/install or repeated restarts.

Recovery decisions must already be in the independent runner when this session
depends on the affected relay. Restore the retained definition/build after a
confirmed replacement failure. Do not restore stale credential snapshots.

Verify the running executable and digest, Comradex-specific readiness on every
configured listener, and a small request through each affected inference path.
A PID, open port, installation message, or usage response alone is insufficient.
Update CLI symlinks and deployment records after the service is verified. Retain
the previous executable until recovery is no longer needed.

Read `src/service.rs`, account lifecycle calls in `src/main.rs`, and the README's
macOS service section for current behavior.
