#!/usr/bin/env python3
"""Turns the artifacts of a native end-to-end run into the measured results the
README figures are built from (docs/results/*.json).

Standard library only. Run from the repository root after `make e2e-native`:

    python3 docs/results/collect.py                      # reads .e2e/native, writes docs/results
    python3 docs/results/collect.py --trace .e2e/trace.json   # the waterfall, from shadowtap trace --json

Only aggregates are written: counts, percentiles and redaction totals, never a
request, a record or a value from the run.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
E2E = os.path.join(ROOT, ".e2e", "native")
OUT = os.path.join(ROOT, "docs", "results")

LATENCY = re.compile(r"latency:\s+p50 (\S+)\s+p95 (\S+)\s+max (\S+)")
REQUESTS = re.compile(r"(\d+) requests in")


def millis(s):
    """Converts a Go duration such as 935µs, 1.57ms or 1.2s to milliseconds."""
    for unit, factor in (("µs", 1 / 1000), ("ms", 1), ("s", 1000)):
        if s.endswith(unit):
            return round(float(s[: -len(unit)]) * factor, 3)
    raise ValueError(s)


def loadgen(name):
    with open(os.path.join(E2E, f"loadgen-{name}.log")) as f:
        text = f.read()
    m = LATENCY.search(text)
    n = REQUESTS.search(text)
    return {"p50": millis(m.group(1)), "p95": millis(m.group(2)), "max": millis(m.group(3)), "requests": int(n.group(1)) if n else None}


def capture():
    redactions, modes, total = {}, {}, 0
    with open(os.path.join(E2E, "captures.ndjson")) as f:
        for line in f:
            if not line.strip():
                continue
            x = json.loads(line)
            total += 1
            modes[x["source"]["mode"]] = modes.get(x["source"]["mode"], 0) + 1
            for kind, n in (x.get("redactions") or {}).items():
                redactions[kind] = redactions.get(kind, 0) + n
    with open(os.path.join(E2E, "ledger.txt")) as f:
        values = sum(1 for line in f if line.strip())
    leaked = 0
    found = subprocess.run(["grep", "-c", "-F", "-f", os.path.join(E2E, "ledger.txt"), os.path.join(E2E, "captures.ndjson")], capture_output=True, text=True)
    if found.stdout.strip().isdigit():
        leaked = int(found.stdout.strip())
    audit_ok = os.path.exists(os.path.join(E2E, "check-pii.txt")) and os.path.getsize(os.path.join(E2E, "check-pii.txt")) == 0
    lat = {name: loadgen(name) for name in ("control", "observe", "inline")}
    return {
        "requests": sum(modes.values()),
        "captured": total,
        "modes": modes,
        "pii_values_sent": values,
        "pii_values_found_in_storage": leaked,
        "pii_audit_clean": audit_ok,
        "redactions": dict(sorted(redactions.items(), key=lambda kv: -kv[1])),
        "latency_ms": {k: {"p50": v["p50"], "p95": v["p95"], "max": v["max"]} for k, v in lat.items()},
        "requests_per_listener": modes.get("observe", 0),
    }


def replay():
    def summary(name):
        with open(os.path.join(E2E, f"replay-{name}.json")) as f:
            r = json.load(f)
        classes = r.get("classes") or r.get("totals", {}).get("classes") or {}
        return r, classes

    same, same_classes = summary("v1")
    cand, cand_classes = summary("v2")
    with open(os.path.join(E2E, "replay-v2.txt")) as f:
        text = f.read()
    regressions = []
    if "phone_number" in text:
        regressions.append("renamed field phone → phone_number")
    if "status changed" in text:
        regressions.append("422 became 400 on POST /api/orders")
    if "latency regression" in text:
        regressions.append("+40 ms on POST /api/orders")
    def p95(run, side):
        return round(run["latency"][side]["p95_us"] / 1000, 2)

    return {
        "same": {
            "replayed": sum(same_classes.values()),
            "classes": same_classes,
            "gate": "passed" if same["gate"]["passed"] else "failed",
            "p95_ms": {"production": p95(same, "prod"), "baseline": p95(same, "baseline"), "candidate": p95(same, "candidate")},
        },
        "candidate": {
            "replayed": sum(cand_classes.values()),
            "classes": cand_classes,
            "gate": "passed" if cand["gate"]["passed"] else "failed",
            "gate_failures": cand["gate"].get("failures", []),
            "regressions": regressions,
            "p95_ms": {"production": p95(cand, "prod"), "baseline": p95(cand, "baseline"), "candidate": p95(cand, "candidate")},
            "noise_suppressed": cand.get("suppressed_noise"),
            "shared_with_baseline_suppressed": cand.get("suppressed_baseline"),
            "noisy_pointers": cand.get("noise"),
        },
        "speed": same.get("speed"),
        "schedule": {"sent": cand["schedule"]["sent"], "dropped": cand["schedule"]["dropped"], "lag_p99_us": cand["schedule"]["lag_p99_us"], "original_span_s": round(cand["schedule"]["original_span_us"] / 1e6, 1), "took_s": round(cand["schedule"]["took_us"] / 1e6, 1)},
    }


def trace(path):
    """Flattens the JSON of `shadowtap trace --json` into the rows of the waterfall figure."""
    with open(path) as f:
        d = json.load(f)
    rows = []

    def walk(span, depth):
        if not span.get("synthetic"):
            rows.append({
                "service": span.get("service", ""),
                "name": span.get("name", ""),
                "kind": span.get("kind", "").lower(),
                "start_us": span.get("offset_ns", 0) // 1000,
                "duration_us": span.get("duration_ns", 0) // 1000,
                "depth": depth,
            })
            depth += 1
        for child in span.get("children", []):
            walk(child, depth)

    roots = d.get("spans") or []
    for root in roots if isinstance(roots, list) else [roots]:
        walk(root, 0)
    start = d["started_at"]
    for c in d.get("captures", []):
        offset = iso_ns(c["started_at"]) - iso_ns(start)
        rows.append({"service": c["service"], "name": "capture " + c["route"], "kind": "capture", "start_us": max(offset, 0) // 1000, "duration_us": c["duration_ns"] // 1000, "depth": 1})
    return {
        "trace_id": d["trace_id"],
        "services": len(d.get("services", [])),
        "spans": rows,
        "span_count": d.get("span_count", 0),
        "logs": d.get("log_count", 0),
        "captures": len(d.get("captures", [])),
        "diffs": len(d.get("diffs", [])),
        "duration_us": d.get("duration_ns", 0) // 1000,
    }


def iso_ns(s):
    """Nanoseconds since the epoch of an RFC 3339 UTC timestamp with any fraction."""
    from datetime import datetime, timezone
    head, _, frac = s.rstrip("Z").partition(".")
    base = datetime.strptime(head, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return int(base.timestamp()) * 10**9 + int((frac + "000000000")[:9])


def main(argv):
    os.makedirs(OUT, exist_ok=True)
    results = {}
    if "--trace" in argv:
        results["trace.json"] = trace(argv[argv.index("--trace") + 1])
    else:
        results.update({"capture.json": capture(), "replay.json": replay()})
    for name, data in results.items():
        with open(os.path.join(OUT, name), "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.write("\n")
        print("wrote", os.path.relpath(os.path.join(OUT, name), ROOT))


if __name__ == "__main__":
    main(sys.argv[1:])
