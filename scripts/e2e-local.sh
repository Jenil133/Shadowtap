#!/usr/bin/env bash
#
# End-to-end test of the local capture pipeline (needs Docker):
#
#   docker compose up (Kafka, tap, demo-api, Envoy)
#   -> loadgen with seeded fake PII through Envoy's observe (:10000) and inline (:10001) listeners
#   -> shadowtap tail reads the captures topic back
#   -> assert captures exist for both modes and none of the PII the loadgen sent reached Kafka
#
# Usage: scripts/e2e-local.sh   (or: make e2e)
#
#   KEEP=1           leave the stack running afterwards (default: tear it down on exit)
#   E2E_REQUESTS=300 requests per listener
#   E2E_SETTLE=5     seconds to let the tap deliver before the topic is read
#
# Raw PII never reaches the console: leak checks print counts only, and tool output that could
# hold request data goes to files under .e2e/.

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

compose_file=deploy/local/compose.yaml
out=.e2e
bin=$out/bin
brokers=localhost:29092
requests=${E2E_REQUESTS:-300}
settle=${E2E_SETTLE:-5}
ready_timeout=120
idle_timeout=15s

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
			# Logs go to a file: demo-api and Envoy see raw (fake) personal data.
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

# count prints the number of lines of file matching an extended regexp (0 when none match).
count() { grep -cE "$1" "$2" || true; }

http_ok() { curl -fsS -o /dev/null --max-time 3 "$1" 2>/dev/null; }

# service_running fails only when compose reports the service as not running; a compose too
# old to filter by status counts as running.
service_running() {
	local ids
	ids=$(compose ps --status running --quiet "$1" 2>/dev/null) || return 0
	[ -n "$ids" ]
}

wait_for_stack() {
	local deadline svc
	deadline=$(($(date +%s) + ready_timeout))
	while :; do
		if http_ok http://localhost:9090/readyz &&
			http_ok http://localhost:9901/ready &&
			http_ok http://localhost:10000/healthz; then
			return 0
		fi
		for svc in kafka tap demo-api envoy; do
			if ! service_running "$svc"; then
				warn "service $svc is not running"
				return 1
			fi
		done
		if [ "$(date +%s)" -ge "$deadline" ]; then
			warn "not ready after ${ready_timeout}s"
			return 1
		fi
		sleep 2
	done
}

finish() {
	printf '\n%d passed, %d failed\n' "$passed" "$failed"
	if [ "$failed" -gt 0 ]; then
		printf 'E2E FAILED (artifacts in %s/)\n' "$out"
		exit 1
	fi
	printf 'E2E PASSED\n'
	exit 0
}

# --- prerequisites -----------------------------------------------------------------------

command -v docker >/dev/null 2>&1 || die "docker is required"
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
docker info >/dev/null 2>&1 || die "cannot reach the Docker daemon"
command -v go >/dev/null 2>&1 || die "go is required"
command -v curl >/dev/null 2>&1 || die "curl is required"

rm -rf "$out"
mkdir -p "$bin"

# Built rather than `go run`: compile errors surface before the stack starts, and `go run`
# would turn tail's exit status 3 (PII found) into 1.
log "Building loadgen and shadowtap"
go build -o "$bin/" ./examples/demo/cmd/loadgen ./cmd/shadowtap

# --- stack -------------------------------------------------------------------------------

log "Starting the local stack ($compose_file)"
stack_started=1
up_help=$(compose up --help 2>/dev/null || true)
case $up_help in
*--wait-timeout*) wait_flags="--wait --wait-timeout 180" ;;
*--wait*) wait_flags="--wait" ;;
*) wait_flags="" ;;
esac
# shellcheck disable=SC2086 # wait_flags is a list of flags
if ! compose up -d --build $wait_flags; then
	[ -n "$wait_flags" ] || die "docker compose up failed"
	warn "docker compose up --wait did not succeed; polling the readiness endpoints instead"
fi

log "Waiting for tap /readyz, Envoy /ready and demo-api through Envoy (up to ${ready_timeout}s)"
if wait_for_stack; then
	pass "stack is ready"
else
	fail "stack did not become ready"
	finish
fi

# --- traffic -----------------------------------------------------------------------------

run_loadgen() { # mode port seed
	log "Sending $requests requests through the $1 listener (:$2)"
	if "$bin/loadgen" --target "http://localhost:$2" --requests "$requests" --seed "$3" \
		--pii-out "$out/pii-$1.txt" >"$out/loadgen-$1.log" 2>&1; then
		pass "loadgen finished against the $1 listener"
	else
		fail "loadgen against the $1 listener exited non-zero (see $out/loadgen-$1.log)"
	fi
}
run_loadgen observe 10000 1
run_loadgen inline 10001 2

log "Giving the tap ${settle}s to deliver"
sleep "$settle"

# --- assertions --------------------------------------------------------------------------

captures=$out/captures.ndjson
log "Reading $captures from $brokers (stops after $idle_timeout without records)"
if ! "$bin/shadowtap" tail --brokers "$brokers" --from earliest --timeout "$idle_timeout" \
	--format json >"$captures"; then
	fail "shadowtap tail exited non-zero"
fi

total=$(count . "$captures")
if [ "$total" -gt 0 ]; then
	pass "$total captures in Kafka"
else
	fail "no captures in Kafka"
fi

for mode in observe inline; do
	n=$(count "\"mode\"[[:space:]]*:[[:space:]]*\"$mode\"" "$captures")
	if [ "$n" -gt 0 ]; then
		pass "$n captures in $mode mode"
	else
		fail "no captures in $mode mode"
	fi
done

# Empty lines are dropped: an empty pattern would match every capture.
ledger=$out/pii-ledger.txt
grep -hv '^[[:space:]]*$' "$out"/pii-*.txt 2>/dev/null | sort -u >"$ledger" || true
values=$(count . "$ledger")
if [ "$values" -eq 0 ]; then
	fail "the PII ledgers are empty or missing; nothing to check against"
else
	leaked=$(grep -c -F -f "$ledger" "$captures" || true)
	if [ "$leaked" -eq 0 ]; then
		pass "none of the $values PII values sent appear in the captures"
	else
		distinct=$(grep -o -F -f "$ledger" "$captures" | sort -u | wc -l | tr -d '[:space:]')
		fail "$distinct of the $values PII values sent leaked into $leaked captures (inspect $captures privately)"
	fi
fi

# Findings name capture ids, fields and kinds, never values; they still go to a file because
# there can be many. The audit summary on stderr is printed.
status=0
"$bin/shadowtap" tail --brokers "$brokers" --from earliest --timeout "$idle_timeout" \
	--check-pii >"$out/check-pii.txt" || status=$?
case $status in
0) pass "shadowtap tail --check-pii found no unscrubbed PII" ;;
3) fail "shadowtap tail --check-pii found unscrubbed PII (findings in $out/check-pii.txt)" ;;
*) fail "shadowtap tail --check-pii failed with exit status $status" ;;
esac

finish
