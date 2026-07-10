# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Compare a KRaft JMH result against a stored baseline (``--mode compare``), or refresh that
baseline (``--mode update``). Used by ``.github/workflows/kraft-benchmarks.yml``.

The baseline lives on the ``benchmark-results`` orphan branch as one JSON file per benchmark
record (a record = benchmark method x @Param combination). Each file holds:

  * ``counters``: the deterministic per-operation values plus ``gc.alloc.rate.norm`` (allocated
    bytes per op). These are code-derived and machine-independent, so they are the HARD GATE:
    in compare mode a mismatch fails the check (unless overridden).
  * ``history``: a rolling window of the noisy, machine-dependent metrics (clock time and GC
    rate/count/time). These are ADVISORY only: compare averages the window and warns on drift,
    never failing.

Markdown is written to stdout (the workflow redirects it to $GITHUB_STEP_SUMMARY); logs go to
stderr. Styled after .github/scripts/junit.py (argparse + stderr logging + glob), stdlib only.
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stderr)
_handler.setLevel(logging.INFO)
logger.addHandler(_handler)

# JMH prefixes profiler ("-prof") secondary metrics with a middle dot, e.g. "·gc.alloc.rate.norm".
GC_PREFIX = "·"
ALLOC_NORM_KEY = "gc.alloc.rate.norm"
NOISY_GC_KEYS = ("gc.alloc.rate", "gc.count", "gc.time")
SCORE_KEY = "score_ns_op"

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"


def parse_args():
    parser = argparse.ArgumentParser(description="KRaft JMH benchmark regression gate")
    parser.add_argument("--result", required=True,
                        help="Path to the JMH result JSON produced by this run")
    parser.add_argument("--baseline-dir", required=True,
                        help="Directory of per-benchmark baseline JSON files")
    parser.add_argument("--mode", required=True, choices=["compare", "update"])
    parser.add_argument("--override", default="false",
                        help="'true' downgrades a hard-gate failure to a warning (compare mode)")
    parser.add_argument("--history-window", type=int, default=30,
                        help="Max number of noisy-metric samples to retain per benchmark")
    parser.add_argument("--counter-abs-tol", type=float, default=1e-6,
                        help="Absolute tolerance for the deterministic per-op counters")
    parser.add_argument("--alloc-norm-rel-tol", type=float, default=0.01,
                        help="Relative tolerance for gc.alloc.rate.norm (allocated bytes/op)")
    parser.add_argument("--noisy-rel-tol", type=float, default=0.20,
                        help="Relative tolerance before an advisory (time/GC) drift is flagged")
    return parser.parse_args()


def as_bool(value):
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def short_name(benchmark):
    # org.apache.kafka.jmh.raft.ElectionBenchmarks.electLeader -> ElectionBenchmarks.electLeader
    return ".".join(benchmark.split(".")[-2:])


def record_filename(benchmark, params):
    suffix = ""
    if params:
        suffix = "__" + "_".join(f"{key}-{params[key]}" for key in sorted(params))
    return re.sub(r"[^A-Za-z0-9._-]", "_", short_name(benchmark) + suffix) + ".json"


def secondary_scores(record):
    """secondaryMetrics -> {name (leading middle-dot stripped): score}."""
    scores = {}
    for name, body in (record.get("secondaryMetrics") or {}).items():
        key = name[len(GC_PREFIX):] if name.startswith(GC_PREFIX) else name
        scores[key] = body.get("score")
    return scores


def extract(record):
    """Split one JMH record into (deterministic counters, alloc-norm, noisy gc, score)."""
    scores = secondary_scores(record)
    # The @AuxCounters values are the ones named "*PerOp"; this auto-includes any future counter
    # and excludes the raw "operations" count (which is noisy under AverageTime).
    counters = {k: v for k, v in scores.items() if k.endswith("PerOp")}
    alloc_norm = scores.get(ALLOC_NORM_KEY)
    gc = {k: scores[k] for k in NOISY_GC_KEYS if scores.get(k) is not None}
    score = (record.get("primaryMetric") or {}).get("score")
    return counters, alloc_norm, gc, score


def load_results(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def load_baseline(baseline_dir, filename):
    path = os.path.join(baseline_dir, filename)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def within(current, baseline, abs_tol, rel_tol):
    if current is None or baseline is None:
        return current == baseline
    if abs(current - baseline) <= abs_tol:
        return True
    denom = max(abs(current), abs(baseline), 1e-12)
    return abs(current - baseline) / denom <= rel_tol


def fmt(value):
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def history_mean(history, key):
    values = [entry[key] for entry in history if entry.get(key) is not None]
    return sum(values) / len(values) if values else None


def mode_compare(records, args):
    override = as_bool(args.override)
    hard_rows = []      # (bench, metric, baseline, current, ok)
    soft_rows = []      # (bench, metric, baseline_avg, current, ok)
    notes = []
    failed = False

    for record in records:
        benchmark = record["benchmark"]
        params = record.get("params") or {}
        label = short_name(benchmark) + (
            " [" + ", ".join(f"{k}={params[k]}" for k in sorted(params)) + "]" if params else "")

        counters, alloc_norm, gc, score = extract(record)
        baseline = load_baseline(args.baseline_dir, record_filename(benchmark, params))
        if baseline is None:
            notes.append(f"`{label}`: no baseline yet — recorded on the next trunk update (not a failure).")
            continue

        base_counters = {k: v for k, v in baseline.get("counters", {}).items() if k.endswith("PerOp")}
        base_alloc = baseline.get("counters", {}).get(ALLOC_NORM_KEY)

        # Hard gate: deterministic per-op counters (exact) ...
        for key in sorted(set(counters) | set(base_counters)):
            ok = within(counters.get(key), base_counters.get(key), args.counter_abs_tol, 0.0)
            failed = failed or not ok
            hard_rows.append((label, key, base_counters.get(key), counters.get(key), ok))
        # ... and allocated bytes per op (tiny relative tolerance).
        if alloc_norm is not None or base_alloc is not None:
            ok = within(alloc_norm, base_alloc, 0.0, args.alloc_norm_rel_tol)
            failed = failed or not ok
            hard_rows.append((label, ALLOC_NORM_KEY, base_alloc, alloc_norm, ok))

        # Soft signal: clock time and noisy GC vs the mean of the stored window.
        history = baseline.get("history", [])
        soft_current = {SCORE_KEY: score}
        soft_current.update(gc)
        for key in (SCORE_KEY,) + NOISY_GC_KEYS:
            if key not in soft_current or soft_current[key] is None:
                continue
            mean = history_mean(history, key)
            if mean is None:
                continue
            ok = within(soft_current[key], mean, 0.0, args.noisy_rel_tol)
            soft_rows.append((label, key, mean, soft_current[key], ok))

    print(render(hard_rows, soft_rows, notes, failed, override))
    if failed and not override:
        logger.error("Hard-gate counter mismatch detected; failing the check.")
        return 1
    if failed and override:
        logger.warning("Hard-gate mismatch present but 'benchmark-override' label set; passing with a warning.")
    return 0


def render(hard_rows, soft_rows, notes, failed, override):
    lines = ["## KRaft benchmark regression report", ""]
    if not hard_rows:
        lines.append("_No baseline to compare against yet — recorded on the next trunk update._")
    elif failed and override:
        lines.append(f"> {WARN} **Counter change detected, but overridden** via the `benchmark-override` "
                     "label — passing. The baseline self-updates when this merges to trunk.")
    elif failed:
        lines.append(f"> {FAIL} **Deterministic counter regression** — this is a behavioral change, not noise. "
                     "Fix it, or add the `benchmark-override` label if the change is intentional.")
    else:
        lines.append(f"> {PASS} Deterministic counters match the baseline.")
    lines.append("")

    if hard_rows:
        lines += ["### Deterministic counters (hard gate)", "",
                  "| Benchmark | Metric | Baseline | Current | |",
                  "|---|---|---:|---:|:--:|"]
        for label, metric, base, cur, ok in hard_rows:
            lines.append(f"| {label} | {metric} | {fmt(base)} | {fmt(cur)} | {PASS if ok else FAIL} |")
        lines.append("")

    if soft_rows:
        lines += ["### Advisory metrics — time & GC (averaged over history; never fails)", "",
                  "| Benchmark | Metric | Baseline avg | Current | |",
                  "|---|---|---:|---:|:--:|"]
        for label, metric, base, cur, ok in soft_rows:
            lines.append(f"| {label} | {metric} | {fmt(base)} | {fmt(cur)} | {PASS if ok else WARN} |")
        lines.append("")

    if notes:
        lines += ["### Notes", ""] + [f"- {note}" for note in notes] + [""]
    return "\n".join(lines)


def mode_update(records, args):
    os.makedirs(args.baseline_dir, exist_ok=True)
    sha = os.getenv("GITHUB_SHA", "local")
    date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated = []

    for record in records:
        benchmark = record["benchmark"]
        params = record.get("params") or {}
        counters, alloc_norm, gc, score = extract(record)

        filename = record_filename(benchmark, params)
        existing = load_baseline(args.baseline_dir, filename) or {}
        history = existing.get("history", [])

        baseline_counters = dict(counters)
        if alloc_norm is not None:
            baseline_counters[ALLOC_NORM_KEY] = alloc_norm

        sample = {"sha": sha, "date": date, SCORE_KEY: score}
        sample.update(gc)
        history.append(sample)
        history = history[-args.history_window:]

        payload = {
            "benchmark": benchmark,
            "params": params,
            "counters": baseline_counters,
            "history": history,
        }
        # Deterministic serialization so commit-only-if-changed no-ops when nothing moved.
        content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        path = os.path.join(args.baseline_dir, filename)
        if not (os.path.exists(path) and open(path, encoding="utf-8").read() == content):
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(content)
        updated.append((short_name(benchmark), filename, len(baseline_counters), len(history)))

    lines = ["## KRaft benchmark baseline update", "",
             f"Refreshed {len(updated)} benchmark record(s) for `{sha}`.", "",
             "| Benchmark | File | Counters | History |",
             "|---|---|---:|---:|"]
    for name, filename, ncounters, nhist in updated:
        lines.append(f"| {name} | `{filename}` | {ncounters} | {nhist} |")
    print("\n".join(lines))
    return 0


def main():
    args = parse_args()
    records = load_results(args.result)
    logger.info("Loaded %d benchmark record(s) from %s", len(records), args.result)
    if args.mode == "compare":
        return mode_compare(records, args)
    return mode_update(records, args)


if __name__ == "__main__":
    sys.exit(main())
