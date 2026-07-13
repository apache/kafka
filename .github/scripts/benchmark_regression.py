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

  * ``counters``: the deterministic per-operation values (the ``*PerOp`` @AuxCounters). These are
    pure protocol logic, so they are the HARD GATE: in compare mode a mismatch fails the check (unless overridden).
  * ``history``: a rolling window of the machine-dependent metrics. These are ADVISORY only: compare averages the window and
    warns on drift, never failing.

Markdown is written to stdout (the workflow redirects it to $GITHUB_STEP_SUMMARY); logs go to
stderr.
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# JMH prefixes profiler ("-prof") secondary metrics with a middle dot, e.g. "·gc.alloc.rate.norm".
GC_PREFIX = "·"
# gc.alloc.rate.norm (allocated bytes/op) is advisory, not gated: it is only near-deterministic
# (JIT escape-analysis and warmup drift it across the runner pool), so hard-gating it would flake.
# Only the *PerOp counters (pure logic) are hard-gated.
ADVISORY_GC_KEYS = ("gc.alloc.rate.norm", "gc.alloc.rate", "gc.count", "gc.time")
SCORE_KEY = "score_ns_op"

PASS = "PASSED ✅"
FAIL = "FAILED ❌"
WARN = "WARNING ⚠️"

# One parsed JMH result record (or a stored baseline file), as loaded from JSON.
Record = Dict[str, Any]
# A rendered report row: (benchmark label, metric name, baseline value, current value, within tolerance).
Row = Tuple[str, str, Optional[float], Optional[float], bool]


def parse_args() -> argparse.Namespace:
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
    parser.add_argument("--noisy-rel-tol", type=float, default=0.20,
                        help="Relative tolerance before an advisory (time/GC) drift is flagged")
    return parser.parse_args()


def as_bool(value: str) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def short_name(benchmark: str) -> str:
    # org.apache.kafka.jmh.raft.ElectionBenchmarks.electLeader -> ElectionBenchmarks.electLeader
    return ".".join(benchmark.split(".")[-2:])


def record_filename(benchmark: str, params: Dict[str, str]) -> str:
    suffix = ""
    if params:
        suffix = "__" + "_".join(f"{key}-{params[key]}" for key in sorted(params))
    return re.sub(r"[^A-Za-z0-9._-]", "_", short_name(benchmark) + suffix) + ".json"


def secondary_scores(record: Record) -> Dict[str, Optional[float]]:
    """secondaryMetrics -> {name (leading middle-dot stripped): score}."""
    scores: Dict[str, Optional[float]] = {}
    for name, body in (record.get("secondaryMetrics") or {}).items():
        key = name[len(GC_PREFIX):] if name.startswith(GC_PREFIX) else name
        scores[key] = body.get("score")
    return scores


def extract(record: Record) -> Tuple[Dict[str, float], Dict[str, float], Optional[float]]:
    """Split one JMH record into (deterministic counters, advisory GC metrics, timing score)."""
    scores = secondary_scores(record)
    # The @AuxCounters values are the ones named "*PerOp"; this auto-includes any future counter
    # and excludes the raw "operations" count (which is noisy under AverageTime).
    counters = {k: v for k, v in scores.items() if k.endswith("PerOp")}
    gc = {k: scores[k] for k in ADVISORY_GC_KEYS if scores.get(k) is not None}
    score = (record.get("primaryMetric") or {}).get("score")
    return counters, gc, score


def load_results(path: str) -> List[Record]:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def load_baseline(baseline_dir: str, filename: str) -> Optional[Record]:
    path = os.path.join(baseline_dir, filename)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def within(current: Optional[float], baseline: Optional[float], abs_tol: float, rel_tol: float) -> bool:
    if current is None or baseline is None:
        return current == baseline
    if abs(current - baseline) <= abs_tol:
        return True
    denom = max(abs(current), abs(baseline), 1e-12)
    return abs(current - baseline) / denom <= rel_tol


def fmt(value: Optional[float]) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def history_mean(history: List[Record], key: str) -> Optional[float]:
    values = [entry[key] for entry in history if entry.get(key) is not None]
    return sum(values) / len(values) if values else None


def mode_compare(records: List[Record], args: argparse.Namespace) -> int:
    override = as_bool(args.override)
    hard_rows: List[Row] = []
    soft_rows: List[Row] = []
    notes: List[str] = []
    failed = False

    for record in records:
        benchmark = record["benchmark"]
        params = record.get("params") or {}
        label = short_name(benchmark) + (
            " [" + ", ".join(f"{k}={params[k]}" for k in sorted(params)) + "]" if params else "")

        counters, gc, score = extract(record)
        baseline = load_baseline(args.baseline_dir, record_filename(benchmark, params))
        if baseline is None:
            notes.append(f"`{label}`: no baseline yet — recorded on the next trunk update (not a failure).")
            continue

        base_counters = {k: v for k, v in baseline.get("counters", {}).items() if k.endswith("PerOp")}

        # Hard gate: the deterministic per-op counters only (exact; pure logic, VM-independent).
        for key in sorted(set(counters) | set(base_counters)):
            ok = within(counters.get(key), base_counters.get(key), args.counter_abs_tol, 0.0)
            failed = failed or not ok
            hard_rows.append((label, key, base_counters.get(key), counters.get(key), ok))

        # Advisory: timing and GC (incl. gc.alloc.rate.norm) vs the mean of the stored window.
        history = baseline.get("history", [])
        soft_current = {SCORE_KEY: score}
        soft_current.update(gc)
        for key in (SCORE_KEY,) + ADVISORY_GC_KEYS:
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


def render(hard_rows: List[Row], soft_rows: List[Row], notes: List[str],
           failed: bool, override: bool) -> str:
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


def mode_update(records: List[Record], args: argparse.Namespace) -> int:
    os.makedirs(args.baseline_dir, exist_ok=True)
    sha = os.getenv("GITHUB_SHA", "local")
    date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated: List[Tuple[str, str, int, int]] = []

    for record in records:
        benchmark = record["benchmark"]
        params = record.get("params") or {}
        counters, gc, score = extract(record)

        filename = record_filename(benchmark, params)
        existing = load_baseline(args.baseline_dir, filename) or {}
        history = existing.get("history", [])

        baseline_counters = dict(counters)

        sample: Record = {"sha": sha, "date": date, SCORE_KEY: score}
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
        existing_content = None
        if os.path.exists(path):
            with open(path, encoding="utf-8") as handle:
                existing_content = handle.read()
        if existing_content != content:
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


def main() -> int:
    args = parse_args()
    records = load_results(args.result)
    logger.info("Loaded %d benchmark record(s) from %s", len(records), args.result)
    if args.mode == "compare":
        return mode_compare(records, args)
    return mode_update(records, args)


if __name__ == "__main__":
    sys.exit(main())
