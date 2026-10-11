#!/usr/bin/env python3
"""Generates the README figures from the measured results in docs/results.

Standard library only. Every figure is written twice, as <name>-light.svg and
<name>-dark.svg, with a transparent background and one colour legend across
all of them:

    teal  = what Shadowtap keeps or produces (scrubbed, captured, matched)
    amber = what was sent or is raw (personal values, regressions)
    slate = control / everything else

Run from the repository root:  python3 docs/assets/build.py
"""

from __future__ import annotations

import json
import os
import sys
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RESULTS = os.path.join(ROOT, "docs", "results")
ASSETS = os.path.join(ROOT, "docs", "assets")

FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

THEMES = {
    "light": {"text": "#1F2937", "muted": "#6B7280", "grid": "#E5E7EB", "teal": "#0F9D8A", "amber": "#E8A33D", "slate": "#64748B", "track": "#F1F5F9"},
    "dark": {"text": "#E5E7EB", "muted": "#9CA3AF", "grid": "#374151", "teal": "#2DD4BF", "amber": "#FBBF24", "slate": "#94A3B8", "track": "#1F2937"},
}


def load(name):
    path = os.path.join(RESULTS, name)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def fmt(n):
    if isinstance(n, float):
        return f"{n:,.1f}" if n < 100 else f"{n:,.0f}"
    return f"{n:,}"


class SVG:
    def __init__(self, width, height, theme):
        self.w, self.h, self.t = width, height, theme
        self.parts = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'font-family="{FONT}" font-size="14" fill="{theme["text"]}">'
        ]

    def text(self, x, y, s, size=14, weight="normal", anchor="start", color=None, mono=False):
        color = color or self.t["text"]
        ff = f' font-family="{MONO}"' if mono else ""
        self.parts.append(
            f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" fill="{color}"{ff}>{esc(s)}</text>'
        )

    def rect(self, x, y, w, h, color, rx=4, opacity=1.0):
        self.parts.append(f'<rect x="{x}" y="{y}" width="{max(w, 0)}" height="{h}" rx="{rx}" fill="{color}" opacity="{opacity}"/>')

    def line(self, x1, y1, x2, y2, color, width=1, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{width}"{d}/>')

    def circle(self, cx, cy, r, color):
        self.parts.append(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{color}"/>')

    def done(self):
        self.parts.append("</svg>")
        return "\n".join(self.parts)


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def write(name, render):
    for theme_name, theme in THEMES.items():
        svg = render(theme)
        path = os.path.join(ASSETS, f"{name}-{theme_name}.svg")
        with open(path, "w") as f:
            f.write(svg)
        ET.fromstring(svg)  # well-formed, or this raises
        print("wrote", os.path.relpath(path, ROOT))


# --- figure: what reached Kafka --------------------------------------------------------------

def headline(theme):
    d = load("capture.json")
    sent, stored = d["pii_values_sent"], d["pii_values_found_in_storage"]
    captured, requests = d["captured"], d["requests"]
    s = SVG(960, 230, theme)
    s.text(0, 22, "Personal values that crossed the sidecar", 15, "600")
    s.text(0, 44, "and how many of them Kafka received", 15, "600")
    scale = 760 / max(sent, 1)
    rows = [
        ("sent in requests", sent, theme["amber"]),
        ("found in any stored record", stored, theme["teal"]),
        ("requests captured", captured, theme["slate"]),
    ]
    y = 72
    for label, value, color in rows:
        s.text(0, y + 17, label, 14, color=theme["muted"])
        s.rect(190, y, 760, 24, theme["track"], rx=5)
        w = value * scale if label != "requests captured" else captured * (760 / max(requests, 1))
        if w > 0:
            s.rect(190, y, w, 24, color, rx=5)
        s.text(190 + w + 10 if w < 680 else 190 + w - 10, y + 17, fmt(value) + (f" of {fmt(requests)}" if label == "requests captured" else ""), 14, "600", anchor="start" if w < 680 else "end", color=theme["text"] if w < 680 else "#FFFFFF")
        y += 40
    s.text(0, 215, "A record counts as a leak when a value appears in its JSON, in any URL-decoded form or inside any JSON string of a body.", 12, color=theme["muted"])
    return s.done()


# --- figure: what the tap adds to a request -------------------------------------------------

def overhead(theme):
    d = load("capture.json")
    lat = d["latency_ms"]
    paths = [("no tap (control)", "control", theme["slate"]), ("observe mode", "observe", theme["teal"]), ("inline mode", "inline", theme["amber"])]
    s = SVG(960, 250, theme)
    s.text(0, 20, "Request latency through Envoy, p50 and p95, by capture mode", 15, "600")
    top = max(lat[k][p] for _, k, _ in paths for p in ("p50", "p95")) or 1
    base_x, width, gap = 160, 760, 36
    y = 50
    for label, key, color in paths:
        s.text(0, y + 24, label, 14, color=theme["muted"])
        for i, p in enumerate(("p50", "p95")):
            v = lat[key][p]
            w = width * v / top
            yy = y + i * 18
            s.rect(base_x, yy, width, 14, theme["track"], rx=3)
            s.rect(base_x, yy, w, 14, color, rx=3, opacity=1.0 if p == "p50" else 0.55)
            label = f"{p} {v:.2f} ms"
            if w < width - 110:
                s.text(base_x + w + 8, yy + 11, label, 12, "600")
            else:
                s.text(base_x + w - 8, yy + 11, label, 12, "600", anchor="end", color="#FFFFFF")
        y += 60
    s.text(0, 240, f"{d['requests_per_listener']} requests per listener from the load generator, same demo service behind every listener, one machine.", 12, color=theme["muted"])
    return s.done()


# --- figure: replay verdicts ----------------------------------------------------------------

def replay(theme):
    d = load("replay.json")
    runs = [("v1 against v1", d["same"], theme["teal"]), ("v2 against v1", d["candidate"], theme["amber"])]
    s = SVG(960, 215, theme)
    s.text(0, 20, "Replay verdicts: a release with no change, and one with three regressions", 15, "600")
    y = 48
    for label, run, color in runs:
        total = run["replayed"] or 1
        s.text(0, y + 17, label, 14, color=theme["muted"])
        x = 160
        legend = []
        for cls, n in sorted(run["classes"].items(), key=lambda kv: (kv[0] != "match", kv[0])):
            if n == 0:
                continue
            w = 760 * n / total
            c = theme["teal"] if cls == "match" else (theme["amber"] if "mismatch" in cls else theme["slate"])
            s.rect(x, y, w, 24, c, rx=3)
            if w > 110:
                s.text(x + 8, y + 17, f"{cls} {n}", 12, "600", color="#FFFFFF")
            legend.append(f"{cls} {n}")
            x += w
        s.text(160, y + 38, " · ".join(legend), 11, color=theme["muted"])
        y += 56
    regs = ", ".join(d["candidate"]["regressions"])
    s.text(0, y + 20, f"Regressions reported for v2: {regs}.", 12, color=theme["muted"])
    s.text(0, y + 40, "Noise is learned by sending each safe request to the baseline twice; a field that differs between those two answers is not a regression.", 12, color=theme["muted"])
    return s.done()


# --- figure: one request across every service ------------------------------------------------

def trace(theme):
    d = load("trace.json")
    spans = d["spans"]
    s = SVG(960, 56 + 26 * len(spans) + 34, theme)
    s.text(0, 20, "One request, one query: everything stored for trace " + d["trace_id"][:12] + "…", 15, "600")
    end = max(sp["start_us"] + sp["duration_us"] for sp in spans) or 1
    left, width = 360, 540
    y = 44
    for sp in spans:
        indent = 14 * sp["depth"]
        s.text(indent, y + 14, sp["service"], 12, "600", color=theme["muted"])
        s.text(indent + 130, y + 14, sp["name"][:34], 12, mono=True)
        x = left + width * sp["start_us"] / end
        w = max(width * sp["duration_us"] / end, 2)
        color = theme["amber"] if sp["kind"] == "capture" else (theme["teal"] if sp["service"].startswith("shadowtap-") and sp["kind"] != "server" else theme["slate"])
        s.rect(x, y + 2, w, 16, color, rx=3)
        s.text(x + w + 6 if x + w < left + width - 60 else x - 6, y + 14, f"{sp['duration_us'] / 1000:.1f} ms", 11, color=theme["muted"], anchor="start" if x + w < left + width - 60 else "end")
        y += 26
    s.text(0, y + 18, f"{d['span_count']} spans, {d['captures']} capture record and {d['logs']} log records share this trace id; found {d['services']} service(s).", 12, color=theme["muted"])
    return s.done()


# --- banner: the story in one picture --------------------------------------------------------

def banner(theme):
    """Requests flow through a tap: personal values (amber) go in, pseudonyms (teal) come out;
    below, a replay compares a candidate with production and a trace lines up every hop."""
    t = theme
    s = SVG(1280, 320, theme)
    s.text(56, 128, "Shadowtap", 60, "800")
    s.text(58, 164, "Sampled traffic from the sidecar, personal data scrubbed before it is stored,", 16, color=t["muted"])
    s.text(58, 188, "replayed against the next build and traced across every service.", 16, color=t["muted"])
    for i, word in enumerate(["capture", "scrub", "replay", "trace"]):
        x = 58 + i * 118
        s.rect(x, 214, 104, 28, t["teal"] if i % 2 == 0 else t["slate"], rx=14, opacity=0.14)
        s.text(x + 52, 233, word, 13, "700", anchor="middle", color=t["teal"] if i % 2 == 0 else t["slate"])

    # Lane 1: requests through the tap.
    y = 88
    s.text(700, y - 26, "live requests", 12, "700", color=t["muted"])
    s.line(700, y, 1230, y, t["grid"], 2)
    tap_x = 960
    s.rect(tap_x - 7, y - 40, 14, 80, t["teal"], rx=5)
    s.text(tap_x, y + 58, "tap", 12, "700", anchor="middle", color=t["teal"])
    before = [(715, [1, 0]), (790, [1, 1]), (865, [0, 1])]
    for x, dots in before:
        s.rect(x, y - 14, 60, 28, t["slate"], rx=7, opacity=0.22)
        for j, d in enumerate(dots):
            if d:
                s.circle(x + 18 + j * 24, y, 6, t["amber"])
    after = [(1000, [1, 0]), (1075, [1, 1]), (1150, [0, 1])]
    for x, dots in after:
        s.rect(x, y - 14, 60, 28, t["slate"], rx=7, opacity=0.22)
        for j, d in enumerate(dots):
            if d:
                s.rect(x + 11 + j * 24, y - 6, 14, 12, t["teal"], rx=3)
    s.text(730, y + 36, "personal values", 11, color=t["amber"])
    s.text(1000, y + 36, "keyed pseudonyms", 11, color=t["teal"])

    # Lane 2: replay and diff.
    y = 196
    s.text(700, y - 26, "replay against the candidate", 12, "700", color=t["muted"])
    s.line(700, y, 1230, y, t["grid"], 2)
    s.rect(715, y - 14, 86, 28, t["slate"], rx=7, opacity=0.22)
    s.text(758, y + 5, "production", 11, "700", anchor="middle", color=t["slate"])
    s.rect(835, y - 14, 86, 28, t["slate"], rx=7, opacity=0.22)
    s.text(878, y + 5, "candidate", 11, "700", anchor="middle", color=t["slate"])
    segs = [(t["teal"], 170), (t["teal"], 40), (t["amber"], 22), (t["teal"], 48)]
    x = 955
    for color, w in segs:
        s.rect(x, y - 9, w - 3, 18, color, rx=3)
        x += w
    s.text(955, y + 36, "match · match · regression · match", 11, color=t["muted"])

    # Lane 3: one trace across services, a small waterfall.
    s.text(700, 252, "one request, every service", 12, "700", color=t["muted"])
    bars = [(700, 520, t["slate"]), (740, 300, t["slate"]), (780, 120, t["slate"]), (920, 110, t["slate"]), (1050, 120, t["teal"])]
    for i, (x, w, color) in enumerate(bars):
        s.rect(x, 260 + i * 9, w, 6, color, rx=3)
    s.text(1225, 304, "trace", 11, "700", anchor="end", color=t["teal"])
    return s.done()


FIGURES = {
    "headline": ("capture.json", headline),
    "overhead": ("capture.json", overhead),
    "replay": ("replay.json", replay),
    "trace": ("trace.json", trace),
    "banner": (None, banner),
}


def main(argv):
    only = set(argv[1:])
    for name, (source, render) in FIGURES.items():
        if only and name not in only:
            continue
        if source is not None and load(source) is None:
            print("skip", name, "(no", source + ")")
            continue
        write(name, render)


if __name__ == "__main__":
    main(sys.argv)
