#!/usr/bin/env bash
#
# End-to-end test of the observability chain on the local stack (needs Docker):
#
#   requests with known trace ids through Envoy -> demo gateway -> users -> orders
#   Envoy, the demo services and the tap export spans to the Collector -> Kafka
#   the tap captures the request -> Kafka
#   shadowtap-indexer -> Elasticsearch
#   shadowtap trace <id> must show every hop, the tap's capture span and the capture record
#
# Usage: scripts/e2e-observability.sh   (or: make e2e-observability)
#
#   KEEP=1            leave the stack running afterwards
#   E2E_REQUESTS=50   requests to send
#   E2E_WAIT=90       seconds to wait for the trace to be indexed

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

compose_file=deploy/local/compose.yaml
out=.e2e/observability
bin=$out/bin
brokers=localhost:29092
es=http://localhost:9200
requests=${E2E_REQUESTS:-50}
wait_s=${E2E_WAIT:-90}

passed=0
failed=0
stack_started=0

log() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
die() {
	printf 'ERROR: %s\n' "$*" >&2
	exit 1
}
pass() {
	passed=$((passed + 1))
	printf 'PASS: %s\n' "$*"
}
fail() {
	failed=$((failed + 1))
	printf 'FAIL: %s\n' "$*"
}
compose() { docker compose -f "$compose_file" "$@"; }

cleanup() {
	local status=$?
	if [ "$stack_started" = 1 ]; then
		if [ "$status" -ne 0 ]; then
			compose logs --no-color >"$out/compose.log" 2>&1 || true
			log "Container logs saved to $out/compose.log"
		fi
		if [ "${KEEP:-0}" = 1 ]; then
			log "KEEP=1: stack left running; stop it with: docker compose -f $compose_file down -v"
		else
			log "Tearing down the stack"
			compose down -v --remove-orphans >/dev/null 2>&1 || warn "docker compose down failed"
		fi
	fi
	exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

http_ok() { curl -fsS -o /dev/null --max-time 3 "$1" 2>/dev/null; }

finish() {
	printf '\n%d passed, %d failed\n' "$passed" "$failed"
	if [ "$failed" -gt 0 ]; then
		printf 'E2E (observability) FAILED (artifacts in %s/)\n' "$out"
		exit 1
	fi
	printf 'E2E (observability) PASSED\n'
	exit 0
}

command -v docker >/dev/null 2>&1 || die "docker is required"
docker info >/dev/null 2>&1 || die "cannot reach the Docker daemon"
command -v go >/dev/null 2>&1 || die "go is required"
command -v curl >/dev/null 2>&1 || die "curl is required"

rm -rf "$out"
mkdir -p "$bin"
log "Building loadgen and shadowtap"
go build -o "$bin/" ./examples/demo/cmd/loadgen ./cmd/shadowtap

log "Starting the local stack ($compose_file)"
stack_started=1
compose up -d --build --wait >"$out/up.log" 2>&1 || warn "docker compose up --wait did not succeed; polling instead"
deadline=$(($(date +%s) + 180))
until http_ok http://localhost:9090/readyz && http_ok http://localhost:9091/readyz && http_ok http://localhost:10000/healthz && http_ok "$es/_cluster/health"; do
	[ "$(date +%s)" -lt "$deadline" ] || {
		fail "stack did not become ready"
		finish
	}
	sleep 2
done
pass "stack is ready (tap, indexer, Envoy, Elasticsearch)"

# One request with a trace id of our own, so the lookup needs no guessing; the
# load generator adds the rest of the traffic. The order route crosses all three
# demo services.
trace_id=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
user_id=$(curl -fsS "http://localhost:10000/api/users?limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["users"][0]["id"])')
log "Sending the marked request (trace $trace_id)"
code=$(curl -s -o "$out/order.json" -w '%{http_code}' -X POST "http://localhost:10000/api/orders" \
	-H "Content-Type: application/json" -H "traceparent: 00-$trace_id-00f067aa0ba902b7-01" \
	-d "{\"user_id\":\"$user_id\",\"items\":[{\"sku\":\"A-1\",\"qty\":1,\"price_cents\":1999}],\"payment\":{\"card_number\":\"4111 1111 1111 1111\",\"cvv\":\"737\",\"exp\":\"12/29\"}}")
if [ "$code" = 201 ]; then pass "marked order request answered 201"; else fail "marked order request answered $code"; fi
log "Sending $requests more requests through Envoy"
"$bin/loadgen" --target http://localhost:10000 --requests "$requests" --pii-out "$out/pii.txt" >"$out/loadgen.log" 2>&1 || fail "loadgen exited non-zero"

log "Waiting up to ${wait_s}s for the trace to reach Elasticsearch"
found=0
deadline=$(($(date +%s) + wait_s))
while [ "$(date +%s)" -lt "$deadline" ]; do
	if "$bin/shadowtap" trace --es "$es" --json "$trace_id" >"$out/trace.json" 2>"$out/trace.err"; then
		services=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(len(d["services"]))' "$out/trace.json")
		[ "$services" -ge 4 ] && found=1 && break
	fi
	sleep 3
done
if [ "$found" = 1 ]; then pass "trace indexed"; else
	fail "trace not fully indexed within ${wait_s}s"
	finish
fi

"$bin/shadowtap" trace --es "$es" "$trace_id" >"$out/trace.txt" 2>&1 || fail "shadowtap trace exited non-zero"
python3 - "$out/trace.json" <<'EOF' || fail "trace is missing a hop (see the lines above)"
import json, sys
d = json.load(open(sys.argv[1]))
services = set(d["services"])
spans = []
def walk(s):
    if not s.get("synthetic"):
        spans.append((s.get("service"), s.get("name")))
    for c in s.get("children", []):
        walk(c)
roots = d["spans"]
for r in roots if isinstance(roots, list) else [roots]:
    walk(r)
problems = []
for svc in ("shadowtap-envoy", "demo-gateway", "demo-users", "demo-orders", "shadowtap-tap"):
    if svc not in services:
        problems.append("no spans from " + svc)
if not any(n == "shadowtap.capture" for _, n in spans):
    problems.append("no shadowtap.capture span")
if len(d.get("captures", [])) != 1:
    problems.append("%d capture records, want 1" % len(d.get("captures", [])))
if d.get("log_count", 0) < 1:
    problems.append("no log records")
for p in problems:
    print("  " + p)
print("  services:", sorted(services), "spans:", len(spans), "logs:", d.get("log_count"), "captures:", len(d.get("captures", [])))
sys.exit(1 if problems else 0)
EOF
[ "$failed" -gt 0 ] || pass "one trace holds Envoy, the three demo services, the tap's capture span, its logs and the capture record"

if grep -q -F -f "$out/pii.txt" "$out/trace.txt" "$out/trace.json" 2>/dev/null; then
	fail "personal data from the load generator appears in the trace output"
else
	pass "nothing the load generator sent appears in the trace"
fi
cp "$out/trace.json" "$out/trace-for-docs.json"
finish
