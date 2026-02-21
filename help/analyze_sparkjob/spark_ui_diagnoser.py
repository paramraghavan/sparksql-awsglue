#!/usr/bin/env python3
"""
spark_ui_diagnoser.py
Works best on Spark 2.4+ / 3.x History Server endpoints.
Auto-diagnose Spark apps using Spark UI / History Server REST API:
  - Executor health: GC%, spill/disk used, skew hints
  - Stage bottlenecks: Max/Median ratios (skew), big shuffle tasks, scheduler delay
  - Basic configuration checks: default shuffle partitions (=200), AQE/skew join flags
  - Actionable recommendations: memory/cores/partitions/AQE/dynamic allocation

Examples:
  python spark_ui_diagnoser.py --base-url http://history-server:18080 --app-id <appId>
  python spark_ui_diagnoser.py --base-url http://localhost:4040 --latest
  python spark_ui_diagnoser.py --base-url http://history-server:18080 --list-apps --limit 10

Notes:
  - Spark UI API is typically at: <base-url>/api/v1/...
  - History Server usually: :18080
  - Live Spark UI usually: :4040
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


# -----------------------------
# Helpers
# -----------------------------

def _get_json(url: str, timeout: int = 20) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _fmt_bytes(n: Optional[int]) -> str:
    if n is None:
        return "n/a"
    if n < 0:
        return str(n)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            return f"{f:.1f} {u}"
        f /= 1024.0
    return f"{f:.1f} PB"

def _pct(num: float, den: float) -> Optional[float]:
    if den <= 0:
        return None
    return (num / den) * 100.0

def _safe_div(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b

def _quantile_ratio(max_v: Optional[float], median_v: Optional[float]) -> Optional[float]:
    if max_v is None or median_v is None:
        return None
    return _safe_div(max_v, median_v)

def _is_default_shuffle_partitions(v: Optional[str]) -> bool:
    # Spark stores conf values as strings
    return v is not None and v.strip() == "200"

def _as_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None

def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


# -----------------------------
# Spark UI Client
# -----------------------------

@dataclass
class SparkApp:
    app_id: str
    name: str
    attempts: List[Dict[str, Any]]

class SparkUIClient:
    def __init__(self, base_url: str, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def apps(self) -> List[Dict[str, Any]]:
        return _get_json(f"{self.base_url}/api/v1/applications", timeout=self.timeout)

    def app(self, app_id: str) -> Dict[str, Any]:
        return _get_json(f"{self.base_url}/api/v1/applications/{app_id}", timeout=self.timeout)

    def app_attempts(self, app_id: str) -> List[Dict[str, Any]]:
        return _get_json(f"{self.base_url}/api/v1/applications/{app_id}", timeout=self.timeout)

    def executors(self, app_id: str, attempt_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if attempt_id:
            url = f"{self.base_url}/api/v1/applications/{app_id}/{attempt_id}/executors"
        else:
            url = f"{self.base_url}/api/v1/applications/{app_id}/executors"
        return _get_json(url, timeout=self.timeout)

    def stages(self, app_id: str, attempt_id: Optional[str] = None, status: Optional[str] = None) -> List[Dict[str, Any]]:
        # status: "active" | "complete" | "pending" | "failed"
        if attempt_id:
            url = f"{self.base_url}/api/v1/applications/{app_id}/{attempt_id}/stages"
        else:
            url = f"{self.base_url}/api/v1/applications/{app_id}/stages"
        if status:
            url += f"?status={status}"
        return _get_json(url, timeout=self.timeout)

    def environment(self, app_id: str, attempt_id: Optional[str] = None) -> Dict[str, Any]:
        if attempt_id:
            url = f"{self.base_url}/api/v1/applications/{app_id}/{attempt_id}/environment"
        else:
            url = f"{self.base_url}/api/v1/applications/{app_id}/environment"
        return _get_json(url, timeout=self.timeout)


# -----------------------------
# Diagnosis Logic
# -----------------------------

@dataclass
class Finding:
    severity: str  # "INFO" | "WARN" | "CRIT"
    title: str
    detail: str
    recommendation: Optional[str] = None

def diagnose_executors(execs: List[Dict[str, Any]]) -> Tuple[List[Finding], Dict[str, Any]]:
    findings: List[Finding] = []

    # Filter out driver if present (often id = "driver")
    worker_execs = [e for e in execs if str(e.get("id", "")).lower() != "driver"]

    # Aggregate metrics
    total_task_time = sum(_as_int(e.get("totalDuration")) or 0 for e in worker_execs)
    total_gc_time = sum(_as_int(e.get("totalGCTime")) or 0 for e in worker_execs)
    total_disk_used = sum(_as_int(e.get("diskUsed")) or 0 for e in worker_execs)

    gc_pct = _pct(total_gc_time, total_task_time)  # based on task duration sum
    summary = {
        "executors": len(worker_execs),
        "total_task_time_ms": total_task_time,
        "total_gc_time_ms": total_gc_time,
        "gc_pct_of_task_time": gc_pct,
        "total_disk_used_bytes": total_disk_used,
    }

    # Disk spill signal
    if total_disk_used > 0:
        findings.append(Finding(
            severity="WARN",
            title="Disk spill detected (Disk Used > 0)",
            detail=f"Total Disk Used across executors: {_fmt_bytes(total_disk_used)}. This typically indicates execution memory pressure.",
            recommendation="Increase spark.executor.memory (often +50%), consider reducing spark.executor.cores (aim 4–5), and/or raise spark.memory.fraction (carefully)."
        ))
    else:
        findings.append(Finding(
            severity="INFO",
            title="No disk spill detected (Disk Used = 0)",
            detail="Disk Used is 0 across executors, which is a good sign for memory headroom."
        ))

    # GC pressure
    if gc_pct is not None and gc_pct > 10.0:
        findings.append(Finding(
            severity="WARN",
            title="High GC time (>10% of task time)",
            detail=f"GC time is ~{gc_pct:.1f}% of total task time across executors.",
            recommendation="Increase spark.executor.memory and/or reduce spark.executor.cores (4–5 per executor). Investigate large objects, wide rows, and shuffle partition sizing."
        ))
    elif gc_pct is not None and gc_pct <= 5.0:
        findings.append(Finding(
            severity="INFO",
            title="GC time in healthy range (<5% of task time)",
            detail=f"GC time is ~{gc_pct:.1f}% of total task time."
        ))
    elif gc_pct is not None:
        findings.append(Finding(
            severity="INFO",
            title="GC time moderate (5–10%)",
            detail=f"GC time is ~{gc_pct:.1f}% of total task time."
        ))

    # Task time variance / potential skew proxy:
    # We don't get task quantiles per executor from this endpoint, but can use totalDuration variance.
    durations = [_as_int(e.get("totalDuration")) or 0 for e in worker_execs]
    if durations:
        mx = max(durations)
        med = sorted(durations)[len(durations)//2]
        ratio = _safe_div(mx, med) if med > 0 else None
        if ratio is not None and ratio >= 5.0:
            findings.append(Finding(
                severity="WARN",
                title="Large executor duration variance (possible skew)",
                detail=f"Max executor totalDuration is ~{ratio:.1f}× median. This can happen with data skew or uneven partitioning.",
                recommendation="Confirm skew in Stages task quantiles (Max vs Median). Consider AQE skew join and skew mitigation (salting, repartition, skew hints)."
            ))

    return findings, summary


def _extract_stage_task_quantiles(stage: Dict[str, Any]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Spark stage endpoint returns a 'taskMetrics' object that may include quantiles.
    In /api/v1/applications/<app>/stages, each stage entry often contains:
      - "taskMetrics": { "duration": { "min":..., "median":..., "max":... }, ... }
    Actual shape varies by Spark version; handle best-effort.
    """
    q = {}
    tm = stage.get("taskMetrics") or {}
    for metric_name in ["duration", "schedulerDelay", "shuffleReadTotalBytes", "shuffleWriteBytes"]:
        metric = tm.get(metric_name)
        if isinstance(metric, dict):
            q[metric_name] = {
                "min": _as_float(metric.get("min")),
                "median": _as_float(metric.get("median")),
                "max": _as_float(metric.get("max")),
            }
    return q


def diagnose_stages(stages: List[Dict[str, Any]]) -> Tuple[List[Finding], Dict[str, Any]]:
    findings: List[Finding] = []

    # Focus on stages with task metrics present
    stage_summaries = []
    skewed = 0
    heavy_shuffle = 0
    high_sched_delay = 0

    for s in stages:
        stage_id = s.get("stageId")
        attempt = s.get("attemptId")
        name = s.get("name") or s.get("description") or ""
        status = s.get("status")
        num_tasks = _as_int(s.get("numTasks")) or 0

        quant = _extract_stage_task_quantiles(s)
        dur_ratio = _quantile_ratio(
            (quant.get("duration") or {}).get("max"),
            (quant.get("duration") or {}).get("median"),
        )

        # Scheduler delay signal
        sched = quant.get("schedulerDelay") or {}
        sched_med = sched.get("median")
        if sched_med is not None and sched_med > 1000.0:
            high_sched_delay += 1

        # Shuffle read size per task (approx)
        # If we have shuffleReadTotalBytes median, compare to 200MB+ warning threshold.
        sr = quant.get("shuffleReadTotalBytes") or {}
        sr_med = sr.get("median")
        if sr_med is not None and sr_med > 200 * 1024 * 1024:
            heavy_shuffle += 1

        if dur_ratio is not None and dur_ratio > 5.0:
            skewed += 1

        stage_summaries.append({
            "stageId": stage_id,
            "attemptId": attempt,
            "status": status,
            "numTasks": num_tasks,
            "name": name[:120],
            "duration_ratio_max_over_median": dur_ratio,
            "schedulerDelay_median_ms": sched_med,
            "shuffleRead_median_bytes": sr_med,
        })

    # Findings based on counts
    if skewed > 0:
        findings.append(Finding(
            severity="WARN",
            title="Data skew / stragglers likely (Max duration > 5× median in stage tasks)",
            detail=f"{skewed} stage(s) show strong skew signals using task duration quantiles.",
            recommendation="Enable AQE and skew join: spark.sql.adaptive.enabled=true and spark.sql.adaptive.skewJoin.enabled=true. Then address skew at source: repartition by join keys, salting, skew hints, or targeted filtering."
        ))
    else:
        findings.append(Finding(
            severity="INFO",
            title="No strong skew signal detected from stage task quantiles",
            detail="No stages found where Max duration > 5× Median (based on available task quantiles)."
        ))

    if heavy_shuffle > 0:
        findings.append(Finding(
            severity="WARN",
            title="Heavy shuffle tasks detected (>200MB median shuffle read per task)",
            detail=f"{heavy_shuffle} stage(s) have median shuffle-read per task above ~200MB.",
            recommendation="Increase spark.sql.shuffle.partitions to reduce bytes/task. Rule of thumb: total_shuffle_read / 128MB. Also consider AQE coalescePartitions."
        ))

    if high_sched_delay > 0:
        findings.append(Finding(
            severity="WARN",
            title="High scheduler delay detected (median > 1s)",
            detail=f"{high_sched_delay} stage(s) show median scheduler delay above ~1s, suggesting cluster contention or YARN queue saturation.",
            recommendation="Check YARN queue capacity/limits, cluster load, and executor allocation. Consider dynamic allocation or increasing cluster resources."
        ))

    summary = {
        "stages_total": len(stages),
        "stages_with_skew_signal": skewed,
        "stages_with_heavy_shuffle_signal": heavy_shuffle,
        "stages_with_high_scheduler_delay": high_sched_delay,
        "top_stages_by_skew_ratio": sorted(
            [x for x in stage_summaries if x["duration_ratio_max_over_median"] is not None],
            key=lambda r: r["duration_ratio_max_over_median"],
            reverse=True
        )[:10],
    }

    return findings, summary


def diagnose_environment(env: Dict[str, Any]) -> Tuple[List[Finding], Dict[str, Any]]:
    findings: List[Finding] = []

    # Spark UI returns environment with keys: "sparkProperties", "systemProperties", "classpathEntries"
    spark_props = env.get("sparkProperties") or []
    # sparkProperties is typically list of [key, value]
    props = {k: v for (k, v) in spark_props if isinstance(k, str)}

    shuffle_partitions = props.get("spark.sql.shuffle.partitions")
    aqe = props.get("spark.sql.adaptive.enabled")
    skew_join = props.get("spark.sql.adaptive.skewJoin.enabled")
    dyn_alloc = props.get("spark.dynamicAllocation.enabled")
    exec_mem = props.get("spark.executor.memory")
    exec_cores = props.get("spark.executor.cores")
    mem_fraction = props.get("spark.memory.fraction")
    overhead = props.get("spark.executor.memoryOverhead")

    summary = {
        "spark.sql.shuffle.partitions": shuffle_partitions,
        "spark.sql.adaptive.enabled": aqe,
        "spark.sql.adaptive.skewJoin.enabled": skew_join,
        "spark.dynamicAllocation.enabled": dyn_alloc,
        "spark.executor.memory": exec_mem,
        "spark.executor.cores": exec_cores,
        "spark.memory.fraction": mem_fraction,
        "spark.executor.memoryOverhead": overhead,
    }

    if _is_default_shuffle_partitions(shuffle_partitions):
        findings.append(Finding(
            severity="INFO",
            title="Default shuffle partitions detected (200)",
            detail="spark.sql.shuffle.partitions is set to 200 (default). On large EMR workloads this is often under-partitioned.",
            recommendation="Set shuffle partitions based on evidence: total_shuffle_read / 128MB. Also consider AQE coalescing."
        ))
    else:
        findings.append(Finding(
            severity="INFO",
            title="Shuffle partitions is not the default (or not reported)",
            detail=f"spark.sql.shuffle.partitions = {shuffle_partitions or 'n/a'}"
        ))

    if aqe is None:
        findings.append(Finding(
            severity="INFO",
            title="AQE setting not found in environment",
            detail="spark.sql.adaptive.enabled was not present in the Spark properties listing (it may still be default/implicit depending on distro)."
        ))
    elif str(aqe).lower() != "true":
        findings.append(Finding(
            severity="INFO",
            title="AQE not enabled",
            detail=f"spark.sql.adaptive.enabled = {aqe}",
            recommendation="Consider enabling AQE for shuffle-heavy SQL workloads: spark.sql.adaptive.enabled=true"
        ))
    else:
        findings.append(Finding(
            severity="INFO",
            title="AQE enabled",
            detail=f"spark.sql.adaptive.enabled = {aqe}"
        ))

    if skew_join is not None and str(skew_join).lower() != "true":
        findings.append(Finding(
            severity="INFO",
            title="AQE skew join not enabled",
            detail=f"spark.sql.adaptive.skewJoin.enabled = {skew_join}",
            recommendation="If you see skew/stragglers: spark.sql.adaptive.skewJoin.enabled=true"
        ))

    if dyn_alloc is not None and str(dyn_alloc).lower() != "true":
        findings.append(Finding(
            severity="INFO",
            title="Dynamic allocation not enabled",
            detail=f"spark.dynamicAllocation.enabled = {dyn_alloc}",
            recommendation="If executors are often idle but reserved, enable dynamic allocation: spark.dynamicAllocation.enabled=true"
        ))

    return findings, summary


def print_report(app_meta: Dict[str, Any],
                 env_summary: Dict[str, Any], env_findings: List[Finding],
                 exec_summary: Dict[str, Any], exec_findings: List[Finding],
                 stage_summary: Dict[str, Any], stage_findings: List[Finding]) -> None:

    app_id = app_meta.get("id") or app_meta.get("appId") or "n/a"
    name = app_meta.get("name") or "n/a"

    def section(title: str):
        print("\n" + "=" * 90)
        print(title)
        print("=" * 90)

    def show_findings(findings: List[Finding]):
        # Group in severity order
        order = {"CRIT": 0, "WARN": 1, "INFO": 2}
        for f in sorted(findings, key=lambda x: order.get(x.severity, 9)):
            badge = {"CRIT": "🟥 CRIT", "WARN": "🟧 WARN", "INFO": "🟦 INFO"}.get(f.severity, f.severity)
            print(f"\n{badge} — {f.title}\n  {f.detail}")
            if f.recommendation:
                print(f"  👉 Recommendation: {f.recommendation}")

    section(f"Spark UI Auto-Diagnoser Report — {name} ({app_id})")

    section("Environment (Effective Runtime Configuration)")
    for k, v in env_summary.items():
        print(f"- {k}: {v}")
    show_findings(env_findings)

    section("Executors (Resource Health)")
    print(f"- Executors (workers): {exec_summary.get('executors')}")
    print(f"- Total task time: {exec_summary.get('total_task_time_ms')} ms")
    print(f"- Total GC time: {exec_summary.get('total_gc_time_ms')} ms")
    gc_pct = exec_summary.get("gc_pct_of_task_time")
    print(f"- GC as % of task time: {('n/a' if gc_pct is None else f'{gc_pct:.1f}%')}")
    print(f"- Total disk used: {_fmt_bytes(exec_summary.get('total_disk_used_bytes'))}")
    show_findings(exec_findings)

    section("Stages (Bottlenecks & Skew Signals)")
    print(f"- Total stages inspected: {stage_summary.get('stages_total')}")
    print(f"- Stages w/ skew signal (Max>5×Median): {stage_summary.get('stages_with_skew_signal')}")
    print(f"- Stages w/ heavy shuffle (median >200MB/task): {stage_summary.get('stages_with_heavy_shuffle_signal')}")
    print(f"- Stages w/ high scheduler delay (median >1s): {stage_summary.get('stages_with_high_scheduler_delay')}")
    show_findings(stage_findings)

    top = stage_summary.get("top_stages_by_skew_ratio") or []
    if top:
        print("\nTop stages by skew ratio (Max/Median duration):")
        for r in top:
            ratio = r.get("duration_ratio_max_over_median")
            print(f"  - stageId={r.get('stageId')} attempt={r.get('attemptId')} "
                  f"status={r.get('status')} tasks={r.get('numTasks')} "
                  f"ratio={('n/a' if ratio is None else f'{ratio:.1f}×')} "
                  f"name={r.get('name')}")


# -----------------------------
# CLI
# -----------------------------

def pick_latest_app(apps: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Pick the app with most recent attempt startTime if available; else last in list.
    """
    best = None
    best_ts = None
    for a in apps:
        atts = a.get("attempts") or []
        # attempts are dicts w/ startTime often like "2026-02-21T..."
        # We'll do lexicographic compare (ISO-8601 sorts) if present.
        for att in atts:
            st = att.get("startTime")
            if st:
                if best_ts is None or st > best_ts:
                    best_ts = st
                    best = a
    return best or (apps[-1] if apps else None)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True, help="Spark History Server or Spark UI base URL (e.g., http://host:18080)")
    ap.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--list-apps", action="store_true", help="List applications and exit")
    g.add_argument("--latest", action="store_true", help="Diagnose the most recent application")
    g.add_argument("--app-id", help="Diagnose a specific application ID")

    ap.add_argument("--attempt-id", help="Optional attemptId (if you want a specific attempt); if omitted uses default endpoints")
    ap.add_argument("--limit", type=int, default=10, help="Limit apps shown with --list-apps")

    args = ap.parse_args()

    client = SparkUIClient(args.base_url, timeout=args.timeout)

    try:
        apps = client.apps()
    except Exception as e:
        print(f"Failed to query apps from {args.base_url}: {e}", file=sys.stderr)
        sys.exit(2)

    if args.list_apps:
        # Print concise list
        print(f"Applications at {args.base_url}:")
        for a in apps[: args.limit]:
            app_id = a.get("id") or "n/a"
            name = a.get("name") or "n/a"
            attempts = a.get("attempts") or []
            last = attempts[-1] if attempts else {}
            print(f"- {app_id} | {name} | attempts={len(attempts)} | "
                  f"lastStart={last.get('startTime')} | lastEnd={last.get('endTime')} | completed={last.get('completed')}")
        return

    # Resolve app
    if args.latest:
        chosen = pick_latest_app(apps)
        if not chosen:
            print("No applications found.", file=sys.stderr)
            sys.exit(1)
        app_id = chosen.get("id")
        if not app_id:
            print("Could not resolve latest app id.", file=sys.stderr)
            sys.exit(1)
    else:
        app_id = args.app_id

    # Pull details
    try:
        app_meta = client.app(app_id)
        env = client.environment(app_id, attempt_id=args.attempt_id)
        execs = client.executors(app_id, attempt_id=args.attempt_id)
        stages = client.stages(app_id, attempt_id=args.attempt_id)
    except Exception as e:
        print(f"Failed to query Spark UI APIs for app {app_id}: {e}", file=sys.stderr)
        sys.exit(2)

    env_findings, env_summary = diagnose_environment(env)
    exec_findings, exec_summary = diagnose_executors(execs)
    stage_findings, stage_summary = diagnose_stages(stages)

    print_report(app_meta, env_summary, env_findings, exec_summary, exec_findings, stage_summary, stage_findings)


if __name__ == "__main__":
    main()