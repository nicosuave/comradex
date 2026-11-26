// Opt-in native integration: bun tests/native_hooks.cjs [facade-binary] [claude-binary]
// Uses a local canned Anthropic endpoint, generated configuration and no paid API.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');

const facade = path.resolve(process.argv[2] || 'target/debug/claude-codex-server');
const native = process.argv[3] || process.env.CLAUDE_BINARY || 'claude';
// Optional real-network regression; reads refs only, using normal SSH auth.
const sshRepository = process.env.NATIVE_SSH_REPOSITORY;
const sshCommand = sshRepository && `git ls-remote '${sshRepository.replaceAll("'", "'\\''")}' HEAD`;
const root = fs.mkdtempSync(path.join(process.cwd(), '.native-hooks-'));
const cwd = path.join(root, 'workspace');
const original = path.join(root, 'original');
const outside = path.join(root, 'outside');
const extra = path.join(root, 'extra');
for (const dir of [cwd, original, outside, extra, path.join(original, 'projects')]) fs.mkdirSync(dir, { recursive: true });
execFileSync('git', ['init', '--quiet', cwd]);
fs.symlinkSync(outside, path.join(cwd, 'outside-link'));
function skill(base, name) {
  const dir = path.join(base, 'skills', name);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'SKILL.md'), `---\nname: ${name}\ndescription: Native integration fixture\n---\n${name} body. PROJECT_PATH=\${CLAUDE_PROJECT_DIR}\n`);
}
skill(original, 'personal');
skill(path.join(cwd, '.claude'), 'project-probe');
skill(path.join(cwd, 'nested/.claude'), 'nested-probe');
fs.writeFileSync(path.join(cwd, 'nested/file.txt'), 'fixture');
fs.writeFileSync(path.join(cwd, 'CLAUDE.local.md'), 'ROOT_NATIVE_LOCAL_MARKER');
fs.writeFileSync(path.join(cwd, 'nested/CLAUDE.local.md'), 'NESTED_NATIVE_LOCAL_MARKER');
fs.writeFileSync(path.join(original, 'settings.json'), JSON.stringify({
  autoMemoryEnabled: true,
  permissions: { allow: ['Edit', 'Bash'], additionalDirectories: [outside] },
  sandbox: { filesystem: { allowWrite: [outside] } },
}));
fs.writeFileSync(path.join(original, '.claude.json'), JSON.stringify({ projects: { [cwd]: { hasTrustDialogAccepted: true } } }));
const hookScript = path.join(root, 'project-hook.cjs');
const hookMarker = path.join(root, 'project-hook.json');
fs.writeFileSync(hookScript, `require('node:fs').writeFileSync(${JSON.stringify(hookMarker)}, JSON.stringify({cwd:process.cwd(),project:process.env.CLAUDE_PROJECT_DIR}));`);
fs.writeFileSync(path.join(cwd, '.claude/settings.json'), JSON.stringify({
  permissions: { allow: ['Edit', 'Bash'], additionalDirectories: [outside] },
  sandbox: { filesystem: { allowWrite: [outside] } },
  hooks: { SessionStart: [{ hooks: [{ type: 'command', command: `node '${hookScript}'` }] }] },
}));
fs.writeFileSync(path.join(cwd, '.claude/settings.local.json'), JSON.stringify({ permissions: { additionalDirectories: [outside] } }));

let child;
let turn;
let index = 0;
let classifierCalls = 0;
const snapshots = [];
const nativeResults = [];
const pending = new Map();
const events = [];
let wake;
let serial = 0;
let stderr = '';
let terminalError;

function modelReply(res, request, content, stopReason) {
  const message = { id: `msg_${turn?.label}_${index}`, type: 'message', role: 'assistant', model: request.model,
    content, stop_reason: stopReason, stop_sequence: null, usage: { input_tokens: 100, output_tokens: 20 } };
  if (!request.stream) {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify(message));
  }
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const event = (type, data) => res.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
  event('message_start', { message: { ...message, content: [], stop_reason: null, usage: { input_tokens: 100, output_tokens: 0 } } });
  content.forEach((block, i) => {
    event('content_block_start', { index: i, content_block: block.type === 'tool_use' ? { ...block, input: {} } : { ...block, text: '' } });
    event('content_block_delta', { index: i, delta: block.type === 'tool_use'
      ? { type: 'input_json_delta', partial_json: JSON.stringify(block.input) }
      : { type: 'text_delta', text: block.text } });
    event('content_block_stop', { index: i });
  });
  event('message_delta', { delta: { stop_reason: stopReason, stop_sequence: null }, usage: { output_tokens: 20 } });
  event('message_stop', {});
  res.end();
}

const server = http.createServer((req, res) => {
  let raw = '';
  req.on('data', chunk => raw += chunk);
  req.on('end', () => {
    try {
      const request = JSON.parse(raw);
      if (req.url.includes('count_tokens')) { res.writeHead(200); return res.end('{"input_tokens":100}'); }
      if (!request.tools?.some(tool => tool.name === 'Skill')) {
        if (JSON.stringify(request.system).includes('security monitor')) {
          classifierCalls++;
          return modelReply(res, request, [{ type: 'text', text: turn.classifierDeny
            ? '<severity>100</severity><category>Unauthorized Action</category>' : '<severity>0</severity>' }], 'end_turn');
        }
        return modelReply(res, request, [{ type: 'text', text: 'Done.' }], 'end_turn');
      }
      for (const message of request.messages || []) {
        if (Array.isArray(message.content)) for (const block of message.content) {
          if (block.type === 'tool_result') nativeResults.push({label: turn.label, block});
        }
      }
      const text = JSON.stringify(request.system) + JSON.stringify(request.messages);
      snapshots.push({ label: turn.label, index, root: text.includes('ROOT_NATIVE_LOCAL_MARKER'), nested: text.includes('NESTED_NATIVE_LOCAL_MARKER'),
        personalBody: text.includes('personal body.'), projectBody: text.includes('project-probe body.'), nestedBody: text.includes('nested-probe body.'),
        realProjectPath: text.includes('PROJECT_PATH=' + cwd) });
      let step = turn.sequence[index++];
      if (typeof step === 'function') step = step();
      modelReply(res, request, step ? [{ type: 'tool_use', id: `${turn.label}_tool_${index}`, ...step }] : [{ type: 'text', text: 'Done.' }], step ? 'tool_use' : 'end_turn');
    } catch (error) { terminalError = error; res.destroy(error); wake?.(); }
  });
});

function send(value) { child.stdin.write(JSON.stringify(value) + '\n'); }
function rpc(method, params) {
  const id = ++serial;
  return new Promise((resolve, reject) => { pending.set(id, { resolve, reject }); send({ id, method, params }); });
}
async function nextEvent() {
  while (!events.length) {
    if (terminalError) throw terminalError;
    await new Promise(resolve => wake = resolve);
    wake = null;
  }
  return events.shift();
}
// Native reports auto-memory under its (profile) config directory.
const memorySlug = cwd.replace(/[^a-zA-Z0-9]/g, '-');
function profileMemory(slug) {
  const namespace = path.join(root, 'state', 'native-claude');
  const [hash] = fs.readdirSync(namespace);
  const [session] = fs.readdirSync(path.join(namespace, hash));
  return path.join(namespace, hash, session, 'projects', slug, 'memory');
}
function sequence(label) {
  return [
    { name: 'Skill', input: { skill: 'personal' } },
    { name: 'Skill', input: { skill: 'project-probe' } },
    { name: 'Read', input: { file_path: path.join(cwd, 'nested/file.txt') } },
    { name: 'Skill', input: { skill: 'nested-probe' } },
    { name: 'Write', input: { file_path: path.join(cwd, `${label}.txt`), content: 'inside' } },
    { name: 'Write', input: { file_path: path.join(extra, `${label}.txt`), content: 'extra' } },
    { name: 'Write', input: { file_path: path.join(outside, `${label}.txt`), content: 'outside' } },
    { name: 'Write', input: { file_path: path.join(cwd, `outside-link/${label}-link.txt`), content: 'outside' } },
    { name: 'Bash', input: { command: `printf inside > '${cwd}/${label}-bash.txt'`, description: 'Workspace write' } },
    ...(sshCommand ? [{ name: 'Bash', input: { command: sshCommand, timeout: 20000, description: 'Read Git SSH refs through native proxy' } }] : []),
    { name: 'Bash', input: { command: `printf outside > '${outside}/${label}-bash.txt'`, description: 'Sandbox should deny outside write' } },
    { name: 'Bash', input: { command: `printf approved > '${outside}/${label}-escape.txt'`, dangerouslyDisableSandbox: true, description: 'Explicit reviewed outside escape' } },
    () => ({ name: 'Write', input: { file_path: path.join(profileMemory(memorySlug), `${label}.md`), content: 'memory' } }),
    () => ({ name: 'Write', input: { file_path: path.join(profileMemory('-other-project'), `${label}.md`), content: 'other' } }),
  ];
}

async function main() {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const env = { ...process.env, CLAUDE_CONFIG_DIR: original,
    ANTHROPIC_BASE_URL: `http://127.0.0.1:${server.address().port}`, ANTHROPIC_API_KEY: 'local-fixture-not-a-credential',
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1' };
  for (const key of ['CLAUDECODE', 'ANTHROPIC_AUTH_TOKEN', 'CLAUDE_CODE_OAUTH_TOKEN', 'CLAUDE_CODEX_PLUGIN_EXECUTABLE', 'CLAUDE_CODEX_PLUGIN_ARGS']) delete env[key];
  child = spawn(facade, ['app-server', '--stdio', '--claude', native, '--state-dir', path.join(root, 'state'), '--model', 'opus',
    '--claude-arg=--strict-mcp-config', '--claude-arg=--tools=Skill,Read,Write,Bash'], { cwd, env, stdio: ['pipe', 'pipe', 'pipe'], detached: true });
  child.stderr.on('data', bytes => stderr += bytes);
  child.on('exit', (code, signal) => {
    terminalError = new Error(`facade exited ${code}/${signal}: ${stderr}`);
    for (const { reject } of pending.values()) reject(terminalError);
    wake?.();
  });
  let buffer = '';
  child.stdout.on('data', bytes => {
    buffer += bytes;
    let end;
    while ((end = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
      let value;
      try { value = JSON.parse(line); } catch { continue; }
      if (value.id != null && !value.method) {
        const request = pending.get(value.id);
        if (request) { pending.delete(value.id); value.error ? request.reject(new Error(JSON.stringify(value.error))) : request.resolve(value.result); }
      } else if (value.id != null && value.method) {
        // Native reviews memory writes outside auto mode like other outside edits.
        if (turn.label === 'manual' && value.method === 'item/fileChange/requestApproval'
          && value.params.reason.includes(`/projects/${memorySlug}/memory/`)) {
          turn.memoryApprovals++;
          send({ id: value.id, result: { decision: 'accept' } });
          wake?.();
          continue;
        }
        turn.approvals++;
        if (turn.label !== 'manual' || value.method !== 'item/commandExecution/requestApproval') {
          terminalError = new Error(`unexpected human approval: ${JSON.stringify(value)}`);
        }
        send({ id: value.id, result: { decision: turn.label === 'manual' ? 'accept' : 'decline' } });
        wake?.();
      } else { events.push(value); wake?.(); }
    }
  });
  await rpc('initialize', { clientInfo: { name: 'native-hook-fixture', version: '1' }, capabilities: { experimentalApi: true } });
  const thread = await rpc('thread/start', { cwd, model: 'opus', effort: 'low', sandbox: 'workspace-write', approvalsReviewer: 'user' });
  const threadId = thread.thread.id;
  for (const config of [
    { label: 'manual', reviewer: 'user', approval: 'on-request', escape: true },
    { label: 'auto', reviewer: 'auto_review', approval: 'on-request', escape: true },
    { label: 'auto-deny', reviewer: 'auto_review', approval: 'on-request', classifierDeny: true, escape: false },
    { label: 'never', reviewer: 'auto_review', approval: 'never', escape: false },
  ]) {
    turn = { ...config, sequence: sequence(config.label), approvals: 0, memoryApprovals: 0 };
    index = 0;
    const classifiersBefore = classifierCalls;
    await rpc('turn/start', { threadId, approvalsReviewer: config.reviewer, approvalPolicy: config.approval,
      sandboxPolicy: { type: 'workspaceWrite', writableRoots: [extra], networkAccess: true }, input: [{ type: 'text', text: `Run ${config.label} fixture.` }] });
    for (;;) {
      const event = await nextEvent();
      if (terminalError) throw terminalError;
      if (event.method === 'turn/completed') {
        assert.equal(event.params.turn.status, 'completed', JSON.stringify(event));
        break;
      }
    }
    assert.equal(index, turn.sequence.length + 1, 'all canned tool steps executed');
    assert.equal(fs.readFileSync(path.join(cwd, `${config.label}.txt`), 'utf8'), 'inside');
    assert.equal(fs.readFileSync(path.join(extra, `${config.label}.txt`), 'utf8'), 'extra');
    const memory = path.join(original, 'projects', memorySlug, 'memory', `${config.label}.md`);
    const memoryResult = nativeResults.find(result => result.block.tool_use_id === `${config.label}_tool_12`);
    // The host guard leaves the session's auto-memory to the selected native reviewer.
    assert(memoryResult && !JSON.stringify(memoryResult).includes('Host workspace boundary'), 'host guard allows native auto-memory');
    assert.equal(fs.existsSync(memory), config.escape);
    assert.equal(turn.memoryApprovals, config.label === 'manual' ? 1 : 0);
    assert.equal(fs.existsSync(path.join(original, 'projects', '-other-project', 'memory', `${config.label}.md`)), false);
    assert.equal(fs.readFileSync(path.join(cwd, `${config.label}-bash.txt`), 'utf8'), 'inside');
    for (const suffix of ['.txt', '-link.txt', '-bash.txt']) assert.equal(fs.existsSync(path.join(outside, config.label + suffix)), false);
    assert.equal(fs.existsSync(path.join(outside, `${config.label}-escape.txt`)), config.escape);
    assert.equal(turn.approvals, config.label === 'manual' ? 1 : 0);
    assert(JSON.stringify(nativeResults.filter(result => result.label === config.label)).includes('Host workspace boundary'), 'native tools report SDK denial');
    assert(JSON.stringify(nativeResults.filter(result => result.label === config.label)).includes('operation not permitted'), 'native shell sandbox denies outside write');
    if (config.reviewer === 'auto_review' && config.approval !== 'never') assert(classifierCalls > classifiersBefore, 'escape reached native classifier');
    if (sshCommand) assert(nativeResults.some(result => result.label === config.label && !result.block.is_error && /[a-f0-9]{40}\s+HEAD/.test(result.block.content)), 'Git SSH read succeeds inside sandbox');
    const own = snapshots.filter(snapshot => snapshot.label === config.label);
    assert(own.every(snapshot => snapshot.root), 'root local instructions preserved');
    assert(own.some(snapshot => snapshot.nested), 'nested local instructions discovered');
    for (const field of ['personalBody', 'projectBody', 'nestedBody', 'realProjectPath']) assert(own.some(snapshot => snapshot[field]), field);
    if (config.label === 'manual') assert.equal(own[0].personalBody, false, 'skill body must load lazily');
    const hook = JSON.parse(fs.readFileSync(hookMarker));
    assert.equal(hook.cwd, cwd);
    assert.equal(hook.project, cwd);
    console.log(`PASS ${config.label}: native skills/local instructions, two roots, guarded writes, Bash reviewer, resumed policy`);
  }
  if (sshCommand) {
    turn = { label: 'ssh-network-disabled', approvals: 0, sequence: [{ name: 'Bash', input: { command: sshCommand, timeout: 20000, description: 'Network-disabled Git SSH must fail' } }] };
    index = 0;
    await rpc('turn/start', { threadId, approvalsReviewer: 'auto_review', approvalPolicy: 'never',
      sandboxPolicy: { type: 'workspaceWrite', writableRoots: [extra], networkAccess: false }, input: [{ type: 'text', text: 'Check disabled network policy.' }] });
    for (;;) {
      const event = await nextEvent();
      if (terminalError) throw terminalError;
      if (event.method === 'turn/completed') {
        assert.equal(event.params.turn.status, 'completed');
        break;
      }
    }
    assert.equal(turn.approvals, 0);
    assert(nativeResults.some(result => result.label === turn.label && result.block.is_error && result.block.content.includes('native sandbox proxy refused SSH connection')), 'network-disabled proxy must deny Git SSH');
    console.log('PASS Git SSH network-disabled policy');
  }
}

const timeout = setTimeout(() => {
  terminalError = new Error(`native integration timed out: ${stderr}`);
  for (const { reject } of pending.values()) reject(terminalError);
  wake?.();
}, 90000);
main().then(() => console.log('PASS native SDK guard integration')).catch(error => { console.error(error); process.exitCode = 1; }).finally(async () => {
  clearTimeout(timeout);
  if (child?.pid && child.exitCode == null && child.signalCode == null) {
    // Let the facade cancel/reap its separately grouped native child first.
    child.stdin.end();
    let cleanupTimer;
    await Promise.race([once(child, 'exit'), new Promise(resolve => cleanupTimer = setTimeout(resolve, 3000))]);
    clearTimeout(cleanupTimer);
    if (child.exitCode == null && child.signalCode == null) {
      try { process.kill(-child.pid, 'SIGKILL'); } catch {}
      await once(child, 'exit');
    }
  }
  server.close(); server.closeAllConnections();
  fs.rmSync(root, { recursive: true, force: true });
});
