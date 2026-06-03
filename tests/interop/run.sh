#!/usr/bin/env bash
# grpcurl interop harness for chuks_grpc (M20).
#
# Boots tests/interop/interop_server.chuks on an ephemeral port and drives it
# with the real `grpcurl` client over h2c (-plaintext): reflection list, a
# unary call, a server-streaming call, and a rich-error call. Exits non-zero
# if any check fails.
#
# Usage:  tests/interop/run.sh [vm|aot]   (default: vm)
# Env:    CHUKS=<chuks binary>  GRPCURL=<grpcurl binary>
set -u

MODE="${1:-vm}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CHUKS="${CHUKS:-$HOME/chuks/bin/chuks}"
GRPCURL="${GRPCURL:-$(command -v grpcurl || echo "$(go env GOPATH 2>/dev/null)/bin/grpcurl")}"
LOG="$(mktemp)"
BIN=""
PASS=0
FAIL=0
SRV_PID=""

cleanup() {
    [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null
    [ -n "$BIN" ] && rm -f "$BIN"
    rm -f "$LOG"
}
trap cleanup EXIT

check() { # name expected-substring actual
    if printf '%s' "$3" | grep -qF "$2"; then
        echo "PASS  $1"
        PASS=$((PASS + 1))
    else
        echo "FAIL  $1 — expected to contain: $2"
        echo "      got: $3"
        FAIL=$((FAIL + 1))
    fi
}

if [ ! -x "$GRPCURL" ] && ! command -v "$GRPCURL" >/dev/null 2>&1; then
    echo "grpcurl not found (set GRPCURL=...); skipping interop harness" >&2
    exit 0
fi

# Boot the server (stdout/stderr -> log).
if [ "$MODE" = "aot" ]; then
    BIN="$(mktemp /tmp/interop_server.XXXXXX)"
    rm -f "$BIN"
    rm -rf "$HOME/.chuks/cache/builds/"* 2>/dev/null
    echo "building AOT interop server..."
    if ! CHUKS_NO_WARNINGS=1 "$CHUKS" build "$HERE/interop_server.chuks" -o "$BIN" >"$LOG" 2>&1; then
        echo "AOT build failed:"; tr '\r' '\n' <"$LOG"; exit 1
    fi
    "$BIN" >"$LOG" 2>&1 &
    SRV_PID=$!
else
    CHUKS_NO_WARNINGS=1 "$CHUKS" run "$HERE/interop_server.chuks" >"$LOG" 2>&1 &
    SRV_PID=$!
fi
echo "mode: $MODE"

# Wait for the advertised address (carriage-return tolerant).
ADDR=""
for _ in $(seq 1 50); do
    ADDR="$(tr '\r' '\n' <"$LOG" | grep '^INTEROP_ADDR=' | head -1 | cut -d= -f2)"
    [ -n "$ADDR" ] && break
    kill -0 "$SRV_PID" 2>/dev/null || { echo "server exited early:"; tr '\r' '\n' <"$LOG"; exit 1; }
    sleep 0.2
done
[ -z "$ADDR" ] && { echo "no INTEROP_ADDR from server:"; tr '\r' '\n' <"$LOG"; exit 1; }
echo "server @ $ADDR"
echo

# 1) Server reflection: list services (M16).
OUT="$("$GRPCURL" -plaintext "$ADDR" list 2>&1)"
check "reflection list advertises echo.Echo" "echo.Echo" "$OUT"

# 2) Unary Say.
OUT="$("$GRPCURL" -plaintext -import-path "$HERE" -proto echo.proto -d '{"name":"world"}' "$ADDR" echo.Echo/Say 2>&1)"
check "unary Say returns 'hello, world'" "hello, world" "$OUT"

# 3) Server-streaming Count.
OUT="$("$GRPCURL" -plaintext -import-path "$HERE" -proto echo.proto -d '{"name":"x"}' "$ADDR" echo.Echo/Count 2>&1)"
check "server-stream Count emits x0" "x0" "$OUT"
check "server-stream Count emits x2" "x2" "$OUT"

# 4) Rich-error Fail (M18 — status + message reach the wire).
OUT="$("$GRPCURL" -plaintext -import-path "$HERE" -proto echo.proto -d '{"name":"z"}' "$ADDR" echo.Echo/Fail 2>&1)"
check "Fail returns InvalidArgument" "InvalidArgument" "$OUT"
check "Fail carries the error message" "interop failure" "$OUT"

echo
echo "interop: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
