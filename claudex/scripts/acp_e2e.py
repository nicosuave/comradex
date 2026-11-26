# /// script
# requires-python = ">=3.11"
# ///
"""Drive a real ACP agent through the facade as Codex desktop would.

Usage: uv run scripts/acp_e2e.py FACADE_BINARY -- FACADE_ARGS...

Runs: model/list, a plain turn, a resumed turn that depends on the first, and
a turn that writes a file (approving each request). Prints a summary and exits
non-zero if any step fails.
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import queue
import time

DEADLINE = float(os.environ.get("E2E_DEADLINE", "240"))


class Facade:
    def __init__(self, binary, args, state):
        self.process = subprocess.Popen(
            [binary, "app-server", "--stdio", "--state-dir", state, *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            text=True,
        )
        self.frames = queue.Queue()
        self.serial = 0
        self.reviews = []
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.process.stdout:
            self.frames.put(json.loads(line))
        self.frames.put(None)

    def send(self, message):
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def next(self, until):
        frame = self.frames.get(timeout=max(1, until - time.time()))
        if frame is None:
            raise RuntimeError("facade exited")
        return frame

    def request(self, method, params):
        self.serial += 1
        request_id = f"e2e-{self.serial}"
        self.send({"id": request_id, "method": method, "params": params})
        until = time.time() + DEADLINE
        while True:
            frame = self.next(until)
            if frame.get("id") == request_id and "method" not in frame:
                if "error" in frame:
                    raise RuntimeError(f"{method}: {frame['error']}")
                return frame["result"]

    def turn(self, thread, text, approvals):
        result = self.request(
            "turn/start",
            {"threadId": thread, "input": [{"type": "text", "text": text, "text_elements": []}]},
        )
        turn_id = result["turn"]["id"]
        until = time.time() + DEADLINE
        while True:
            frame = self.next(until)
            method = frame.get("method")
            if "id" in frame and method:
                approvals.append(method)
                if method == "item/tool/requestUserInput":
                    answer = {"answers": {"permission": {"answers": ["Allow"]}}}
                elif method == "item/permissions/requestApproval":
                    answer = {"permissions": frame["params"]["permissions"], "scope": "turn"}
                else:
                    answer = {"decision": "accept"}
                self.send({"id": frame["id"], "result": answer})
            elif method == "item/autoApprovalReview/completed":
                self.reviews.append(frame["params"]["review"]["status"])
            elif method == "turn/completed" and frame["params"]["turn"]["id"] == turn_id:
                return frame["params"]["turn"]


def reply(turn):
    return "\n".join(item.get("text", "") for item in turn["items"] if item["type"] == "agentMessage")


def main():
    binary = sys.argv[1]
    args = sys.argv[sys.argv.index("--") + 1 :]
    state = tempfile.mkdtemp(prefix="acp-e2e-")
    workspace = os.path.realpath(tempfile.mkdtemp(prefix="acp-e2e-work-"))
    facade = Facade(binary, args, state)
    ok = True

    def check(name, condition, detail=""):
        nonlocal ok
        ok &= bool(condition)
        print(f"{'PASS' if condition else 'FAIL'} {name} {detail}".rstrip(), flush=True)

    facade.request("initialize", {"clientInfo": {"name": "acp-e2e", "version": "1"},
                                  "capabilities": {"experimentalApi": True}})
    models = facade.request("model/list", {})["data"]
    check("model/list", models, f"{[m['id'] for m in models][:8]}")
    auto_review = os.environ.get("E2E_AUTO_REVIEW") == "1"
    start = {"cwd": workspace, "sandbox": "danger-full-access"}
    if auto_review:
        start["approvalsReviewer"] = "auto_review"
    thread = facade.request("thread/start", start)
    thread_id = thread["thread"]["id"]
    check("thread/start", thread["modelProvider"] != "anthropic", thread["modelProvider"])

    approvals = []
    first = facade.turn(thread_id, "Reply with exactly the word: pineapple", approvals)
    check("plain turn", first["status"] == "completed" and "pineapple" in reply(first).lower(),
          f"status={first['status']} reply={reply(first)[:120]!r} error={first.get('error')}")

    second = facade.turn(thread_id, "What single word did you reply with in your previous message? Answer with just that word.", approvals)
    check("resumed turn remembers", second["status"] == "completed" and "pineapple" in reply(second).lower(),
          f"status={second['status']} reply={reply(second)[:120]!r} error={second.get('error')}")

    third = facade.turn(
        thread_id,
        "Create a file named hello.txt in the current working directory containing exactly the text: hi from acp. Use your file editing tool, then say done.",
        approvals,
    )
    path = os.path.join(workspace, "hello.txt")
    content = open(path).read().strip() if os.path.exists(path) else None
    kinds = sorted({item["type"] for item in third["items"]})
    check("tool turn writes file", third["status"] == "completed" and content == "hi from acp",
          f"status={third['status']} content={content!r} items={kinds} approvals={sorted(set(approvals))} error={third.get('error')}")
    if auto_review:
        check("automatic review decided instead of the user", facade.reviews and not approvals,
              f"reviews={facade.reviews} user approvals={approvals}")

    turns = 3
    chosen = os.environ.get("E2E_MODEL") or next((m["id"] for m in models if not m["isDefault"]), None)
    if chosen:
        approvals.clear()
        result = facade.request(
            "turn/start",
            {"threadId": thread_id, "model": chosen,
             "input": [{"type": "text", "text": "Reply with exactly the word: mango", "text_elements": []}]},
        )
        until = time.time() + DEADLINE
        while True:
            frame = facade.next(until)
            if frame.get("method") == "turn/completed" and frame["params"]["turn"]["id"] == result["turn"]["id"]:
                fourth = frame["params"]["turn"]
                break
        turns += 1
        check(f"turn with model {chosen}", fourth["status"] == "completed" and "mango" in reply(fourth).lower(),
              f"status={fourth['status']} reply={reply(fourth)[:80]!r} error={fourth.get('error')}")

    usage = facade.request("thread/read", {"threadId": thread_id, "includeTurns": True})
    check("thread/read", len(usage["thread"]["turns"]) == turns)
    facade.process.stdin.close()
    facade.process.wait(timeout=30)
    check("facade exits cleanly", facade.process.returncode == 0, str(facade.process.returncode))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
