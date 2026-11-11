#!/usr/bin/env bash
# Runs three ratelimit instances on 127.0.0.1 (no Kubernetes, no Docker) and
# shows the per-client limit holding across them. Usage: scripts/demo-local.sh
set -euo pipefail
cd "$(dirname "$0")/.."

tmp=$(mktemp -d)
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT

go build -o "$tmp/ratelimit" ./examples/ratelimit

REPL=(19091 19092 19093)
HTTP=(18081 18082 18083)
PEERS=$(printf '127.0.0.1:%s,' "${REPL[@]}"); PEERS=${PEERS%,}
for i in 0 1 2; do
  HTTP_ADDR=127.0.0.1:${HTTP[$i]} REPL_ADDR=127.0.0.1:${REPL[$i]} SELF=127.0.0.1:${REPL[$i]} \
    PEERS=$PEERS LIMIT=5 WINDOW_SECONDS=10 \
    "$tmp/ratelimit" 2>"$tmp/pod$i.log" &
  pids+=($!)
done

echo "== waiting for /readyz on all three pods"
for i in 0 1 2; do
  for _ in $(seq 100); do
    curl -fs "127.0.0.1:${HTTP[$i]}/readyz" >/dev/null 2>&1 && break
    sleep 0.1
  done
  curl -fs "127.0.0.1:${HTTP[$i]}/readyz" >/dev/null || { echo "pod $i never became ready"; cat "$tmp/pod$i.log"; exit 1; }
done
sleep 3 # let peer sets refresh (2s) so every pod fans out to the others

echo "== 12 hits for client=alice, round-robin over the 3 pods (limit 5 per 10s)"
sleep $((10 - $(date +%s) % 10)) # start at a window boundary so all hits share a window
allowed=0
for n in $(seq 12); do
  i=$(( (n - 1) % 3 ))
  out=$(curl -s -i "127.0.0.1:${HTTP[$i]}/hit?client=alice" | tr -d '\r')
  code=$(echo "$out" | head -1 | awk '{print $2}')
  pod=$(echo "$out" | awk -F': ' 'tolower($1)=="x-pod"{print $2}')
  [ "$code" = 200 ] && allowed=$((allowed + 1))
  printf 'hit %2d -> pod port %s  X-Pod=%-28s HTTP %s\n' "$n" "${HTTP[$i]}" "$pod" "$code"
  sleep 0.2 # let replication catch up; without it the sum can overshoot
done
echo "== allowed $allowed of 12 (expected 5)"

echo "== /stats on pod 0"
curl -s "127.0.0.1:${HTTP[0]}/stats"; echo

echo "== info logs from pod 0"
grep -E 'peers changed|hydrated|first node|ready' "$tmp/pod0.log" | head -5
[ "$allowed" -ge 5 ] && [ "$allowed" -le 6 ] || { echo "UNEXPECTED allowed count"; exit 1; }
