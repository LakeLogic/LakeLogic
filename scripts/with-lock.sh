#!/usr/bin/env bash
#
# Run a command while holding an exclusive lock on a named resource.
#
#   scripts/with-lock.sh <resource> <expected-minutes> <command...>
#
#   scripts/with-lock.sh colab-lakehouse 25 python -m pytest --nbmake examples/colab/
#   scripts/with-lock.sh playwright 5 npx playwright test
#
# Why this exists, concretely: three notebook suite runs were destroyed in one day
# because two of them shared examples/colab/lakehouse. Each run deleted the
# directory the other was reading, and the results disagreed in ways that looked
# like real failures — hours were spent diagnosing defects that were contention.
# The same day, a Playwright browser could not be driven because another session
# already held its profile.
#
# Checking `ps` before starting is what a careful person does and it failed twice
# before it worked, because the check and the start are not atomic. A lock file
# created with `set -o noclobber` is.
#
# Locks record WHO, WHAT and HOW LONG, so a second agent finds a note rather than a
# mystery. A lock past its stated duration is treated as stale and taken over: a
# killed process must not block the resource forever, which is the failure mode
# that makes people delete lock files reflexively and stop trusting them.
set -uo pipefail

usage() { echo "usage: $0 <resource> <expected-minutes> <command...>" >&2; exit 64; }
[ $# -ge 3 ] || usage

RESOURCE="$1"; MINUTES="$2"; shift 2
case "$RESOURCE" in *[!a-zA-Z0-9_-]*) echo "resource must be [A-Za-z0-9_-]" >&2; exit 64;; esac
case "$MINUTES" in ''|*[!0-9]*) usage;; esac

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
LOCK_DIR="$ROOT/.locks"
LOCK="$LOCK_DIR/$RESOURCE"
mkdir -p "$LOCK_DIR"

now() { date +%s; }

read_field() { [ -f "$LOCK" ] && grep -m1 "^$1=" "$LOCK" 2>/dev/null | cut -d= -f2- || true; }

if [ -f "$LOCK" ]; then
    held_by="$(read_field agent)"
    started="$(read_field started_epoch)"
    expires="$(read_field expires_epoch)"
    if [ -n "$expires" ] && [ "$(now)" -gt "$expires" ]; then
        echo "note: taking over a STALE lock on '$RESOURCE' (held by ${held_by:-unknown}," >&2
        echo "      expired $(( ( $(now) - expires ) / 60 ))m ago). The holder likely died." >&2
        rm -f "$LOCK"
    else
        remaining=$(( ( ${expires:-0} - $(now) ) / 60 ))
        echo "REFUSING: '$RESOURCE' is locked by ${held_by:-unknown}" >&2
        echo "  command : $(read_field command)" >&2
        echo "  started : $(read_field started)" >&2
        echo "  expires : in ~${remaining}m" >&2
        echo "" >&2
        echo "Wait, or pick a resource that is not shared. Do NOT delete the lock to" >&2
        echo "get past it — that is exactly how the contaminated runs happened." >&2
        exit 75   # EX_TEMPFAIL
    fi
fi

# noclobber makes create-if-absent atomic: two agents racing here cannot both win.
if ! ( set -o noclobber; : > "$LOCK" ) 2>/dev/null; then
    echo "REFUSING: lost the race for '$RESOURCE' to another process." >&2
    exit 75
fi

STARTED_EPOCH="$(now)"
{
    echo "agent=${CLAUDE_AGENT_ID:-${USER:-unknown}}@$(hostname 2>/dev/null || echo host)"
    echo "pid=$$"
    echo "command=$*"
    echo "started=$(date -Iseconds 2>/dev/null || date)"
    echo "started_epoch=$STARTED_EPOCH"
    echo "expires_epoch=$(( STARTED_EPOCH + MINUTES * 60 ))"
    echo "expected_minutes=$MINUTES"
} > "$LOCK"

# Released on ANY exit, including Ctrl-C and kill. A lock that outlives its holder
# is the reason people stop trusting locks.
CHILD=""
cleanup() {
    [ -n "$CHILD" ] && kill -TERM "$CHILD" 2>/dev/null
    rm -f "$LOCK"
}
trap cleanup EXIT INT TERM

# Run in the BACKGROUND and wait, rather than in the foreground.
#
# Not a style choice: bash defers a trap until the current foreground command
# returns, so killing this wrapper during a 20-minute test run would leave the
# lock in place for those 20 minutes — the stale-lock behaviour this whole script
# exists to avoid. `wait` is interruptible, so the trap fires immediately.
"$@" &
CHILD=$!
wait "$CHILD"
status=$?
CHILD=""

elapsed=$(( ( $(now) - STARTED_EPOCH ) / 60 ))
if [ "$elapsed" -gt "$MINUTES" ]; then
    echo "note: '$RESOURCE' ran ${elapsed}m against an estimate of ${MINUTES}m." >&2
    echo "      Another agent may have treated the lock as stale and started anyway;" >&2
    echo "      treat these results as suspect." >&2
fi
exit $status
