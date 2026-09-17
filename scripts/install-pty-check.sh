#!/usr/bin/env bash
# PTY regression test for `kite -i`: the PATH prompt must read the answer
# from the controlling terminal (/dev/tty), not from a piped stdin — the
# `curl … | sh` situation. Skips cleanly when python3 is missing.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v python3 >/dev/null 2>&1 || { echo "SKIP install-pty-check: python3 missing"; exit 0; }

BIN=$(realpath "${1:-zig-out/bin/kite}")
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

[ -x "$BIN" ] || { echo "run zig build first" >&2; exit 1; }

KITE="$BIN" PTY_TMP="$TMP" python3 - <<'PY'
import os
import pty
import select
import sys
import time

kite = os.environ["KITE"]
tmp = os.environ["PTY_TMP"]


def run(name, answer, extra=""):
    home = os.path.join(tmp, name)
    os.makedirs(home)
    env = dict(os.environ)
    env.update(HOME=home, SHELL="/bin/bash", PATH="/usr/bin:/bin",
               KITE=kite, BINDIR=os.path.join(tmp, name + "-bin"))
    pid, fd = pty.fork()
    if pid == 0:
        # stdin is a pipe (like `echo … | sh` under curl|sh); stdout and the
        # controlling terminal are the pty.
        os.execvpe("sh", ["sh", "-c",
                          'echo ignored-stdin | "$KITE" -i --dir "$BINDIR" ' + extra], env)
    output = b""
    sent = False
    deadline = time.time() + 10
    status = None
    while time.time() < deadline:
        r, _, _ = select.select([fd], [], [], 0.2)
        if r:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                chunk = b""
            if chunk:
                output += chunk
        if answer is not None and b"[Y/n]" in output and not sent:
            os.write(fd, answer)
            sent = True
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            break
    else:
        os.kill(pid, 9)
        print(f"FAIL {name}: timed out", flush=True)
        sys.exit(1)
    try:
        while True:
            r, _, _ = select.select([fd], [], [], 0.2)
            if not r:
                break
            output += os.read(fd, 4096)
    except OSError:
        pass
    os.close(fd)
    return output.decode("utf-8", "replace"), home


def bashrc_exports(home):
    path = os.path.join(home, ".bashrc")
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return sum(1 for line in f if "export PATH=" in line)


def check(name, cond, detail):
    if not cond:
        print(f"FAIL {name}: {detail}", flush=True)
        sys.exit(1)
    print(f"PASS {name}", flush=True)


# 'n' to the prompt: the hint is printed and nothing is written to .bashrc.
out, home = run("answer-no", b"n\n")
check("answer-no", "Add that line" in out and bashrc_exports(home) == 0,
      f"expected hint and no rc write; got:\n{out}")

# 'y' to the prompt: the line is appended exactly once.
out, home = run("answer-yes", b"y\n")
check("answer-yes", "added —" in out and bashrc_exports(home) == 1,
      f"expected 'added —' and one rc line; got:\n{out}")

# --yes never prompts.
out, home = run("flag-yes", None, "--yes")
check("flag-yes", "[Y/n]" not in out and bashrc_exports(home) == 1,
      f"expected no prompt and one rc line; got:\n{out}")
PY
