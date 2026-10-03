#!/usr/bin/env bash
#
# End-to-end test of the capture pipeline without Docker: a real Envoy (both
# ext_proc modes) in front of demo-api, the shadowtap-tap binary, and an
# in-memory Kafka-protocol broker (scripts/fakekafka).
#
#   envoy (:11000 observe, :11001 inline) -> demo-api (:18080)
#        `-- ext_proc --> shadowtap-tap (:19002, admin :19090) --> fakekafka (:29092)
#
# Requires go, curl and an envoy binary on PATH (brew install envoy, or func-e).
#
# Usage: scripts/e2e-native.sh   (or: make e2e-native)
#   E2E_REQUESTS=300   requests per listener
#
# Raw PII never reaches the console: leak checks print counts only.

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

out=.e2e/native
bin=$out/bin
requests=${E2E_REQUESTS:-300}
brokers=127.0.0.1:29092

passed=0
failed=0
pids=""

log() { printf '==> %s\n' "$*"; }
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

cleanup() {
	local status=$?
	for pid in $pids; do
		kill "$pid" 2>/dev/null || true
	done
	wait 2>/dev/null || true
	exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

start() { # name, command...
	local name=$1
	shift
	"$@" >"$out/$name.log" 2>&1 &
	pids="$pids $!"
}

http_ok() { curl -fsS -o /dev/null --max-time 2 "$1" 2>/dev/null; }

# stat prints the value of an Envoy counter (0 when absent).
stat() { curl -fsS "http://127.0.0.1:19901/stats?filter=^$1\$" | awk -F': ' '{print $2}' | head -n 1 | grep . || echo 0; }

command -v envoy >/dev/null 2>&1 || die "envoy is required on PATH (brew install envoy)"
command -v go >/dev/null 2>&1 || die "go is required"
command -v curl >/dev/null 2>&1 || die "curl is required"

rm -rf "$out"
mkdir -p "$bin"
log "Building binaries"
go build -o "$bin/" ./cmd/shadowtap-tap ./cmd/shadowtap ./examples/demo/cmd/demo-api ./examples/demo/cmd/loadgen ./scripts/fakekafka

# The local stack's Envoy config, moved to loopback ports.
sed -e 's/address: 0\.0\.0\.0/address: 127.0.0.1/' \
	-e 's/address: demo-api$/address: 127.0.0.1/' -e 's/address: tap$/address: 127.0.0.1/' \
	-e 's/port_value: 10000$/port_value: 11000/' -e 's/port_value: 10001$/port_value: 11001/' \
	-e 's/port_value: 9901$/port_value: 19901/' -e 's/port_value: 8080$/port_value: 18080/' \
	-e 's/port_value: 9002$/port_value: 19002/' deploy/local/envoy.yaml >"$out/envoy.yaml"
envoy --mode validate -c "$out/envoy.yaml" >"$out/envoy-validate.log" 2>&1 || die "envoy rejected $out/envoy.yaml (see $out/envoy-validate.log)"

log "Starting fakekafka, demo-api, shadowtap-tap and envoy"
start fakekafka "$bin/fakekafka" --port 29092
start demo-api "$bin/demo-api" --addr 127.0.0.1:18080
start tap env SHADOWTAP_KAFKA_BROKERS=$brokers SHADOWTAP_SCRUB_KEY=native-e2e-scrub-key-not-for-production \
	SHADOWTAP_SAMPLE_RATE=1.0 POD_NAME=tap-native \
	SHADOWTAP_GRPC_ADDR=127.0.0.1:19002 SHADOWTAP_HTTP_ADDR=127.0.0.1:19090 \
	"$bin/shadowtap-tap" --config deploy/local/tap.yaml
start envoy envoy -c "$out/envoy.yaml" --log-level warn --base-id 7

ready=0
for _ in $(seq 1 120); do
	if http_ok http://127.0.0.1:19090/readyz && http_ok http://127.0.0.1:19901/ready &&
		http_ok http://127.0.0.1:11000/healthz && http_ok http://127.0.0.1:11001/healthz; then
		ready=1
		break
	fi
	sleep 0.5
done
[ "$ready" = 1 ] || die "stack not ready after 60s (logs in $out/)"
pass "stack is ready"

sent=0
for mode in observe:11000:1 inline:11001:2; do
	name=${mode%%:*}
	rest=${mode#*:}
	port=${rest%%:*}
	seed=${rest#*:}
	log "Sending $requests requests through the $name listener (:$port)"
	if "$bin/loadgen" --target "http://127.0.0.1:$port" --requests "$requests" --seed "$seed" \
		--pii-out "$out/pii-$name.txt" >"$out/loadgen-$name.log" 2>&1; then
		pass "loadgen finished against the $name listener"
		sent=$((sent + requests))
	else
		fail "loadgen against the $name listener exited non-zero (see $out/loadgen-$name.log)"
	fi
done

captures=$out/captures.ndjson
"$bin/shadowtap" tail --brokers "$brokers" --from earliest --timeout 3s --format json >"$captures" 2>"$out/tail.log" ||
	fail "shadowtap tail exited non-zero"
total=$(grep -c . "$captures" || true)
if [ "$total" -eq "$sent" ]; then
	pass "all $sent requests captured (sample rate 1.0)"
else
	fail "$total captures for $sent requests"
fi
for name in observe inline; do
	n=$(grep -c "\"mode\":\"$name\"" "$captures" || true)
	if [ "$n" -eq "$requests" ]; then pass "$n captures in $name mode"; else fail "$n captures in $name mode, want $requests"; fi
done

grep -hv '^[[:space:]]*$' "$out"/pii-*.txt | sort -u >"$out/ledger.txt"
values=$(grep -c . "$out/ledger.txt" || true)
leaked=$(grep -c -F -f "$out/ledger.txt" "$captures" || true)
if [ "$values" -gt 0 ] && [ "$leaked" -eq 0 ]; then
	pass "none of the $values PII values sent appear in the captures"
else
	fail "$leaked captures contain one of the $values PII values sent (inspect $captures privately)"
fi

status=0
"$bin/shadowtap" tail --brokers "$brokers" --from earliest --timeout 3s --check-pii >"$out/check-pii.txt" 2>"$out/check-pii.log" || status=$?
if [ "$status" -eq 0 ]; then pass "shadowtap tail --check-pii found no unscrubbed PII"; else fail "shadowtap tail --check-pii exited $status (findings in $out/check-pii.txt)"; fi

# Envoy's own view of the ext_proc protocol: every inline message answered, nothing failed.
for s in streams_failed failure_mode_allowed message_timeouts spurious_msgs_received rejected_header_mutations; do
	for listener in observe inline; do
		v=$(stat "http.$listener.ext_proc.$s")
		[ "$v" = 0 ] || fail "envoy http.$listener.ext_proc.$s = $v"
	done
done
sent_inline=$(stat http.inline.ext_proc.stream_msgs_sent)
recv_inline=$(stat http.inline.ext_proc.stream_msgs_received)
if [ "$sent_inline" -gt 0 ] && [ "$sent_inline" = "$recv_inline" ]; then
	pass "envoy: inline mode got one reply for each of its $sent_inline messages, no ext_proc errors"
else
	fail "envoy: inline mode sent $sent_inline messages and received $recv_inline replies"
fi

printf '\n%d passed, %d failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ] || {
	printf 'E2E (native) FAILED (artifacts in %s/)\n' "$out"
	exit 1
}
printf 'E2E (native) PASSED\n'
