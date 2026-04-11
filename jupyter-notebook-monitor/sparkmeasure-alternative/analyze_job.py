#!/usr/bin/env python3
"""
analyze_job.py — Analyze completed Spark jobs from History Server
═══════════════════════════════════════════════════════════════════

Analyze past Spark job metrics and performance issues using the
Spark History Server. No need to run %%measure — just pick a job
and analyze it after it completes.

USAGE
─────
In Jupyter:
    from analyze_job import analyze_job
    analyze_job("app-20240115-0001")  # Analyze by app ID
    analyze_job()                      # Show recent jobs

Command line:
    python analyze_job.py --app-id app-20240115-0001
    python analyze_job.py --recent   # Show last 10 jobs
"""

import json
import urllib.request
import sys
from IPython.display import display, HTML

# ═══════════════════════════════════════════════════════════════
#  HISTORY SERVER API
# ═══════════════════════════════════════════════════════════════

def _get_from_history(path, host="localhost", port=18080):
    """Query Spark History Server REST API."""
    try:
        with urllib.request.urlopen(
                f"http://{host}:{port}{path}", timeout=5) as r:
            return json.loads(r.read())
    except Exception as e:
        return None


def _get_app_info(app_id, host="localhost", port=18080):
    """Get application info from History Server."""
    return _get_from_history(f"/api/v1/applications/{app_id}", host, port)


def _get_app_stages(app_id, host="localhost", port=18080):
    """Get all stages for a completed application."""
    return _get_from_history(
        f"/api/v1/applications/{app_id}/stages", host, port) or []


def _list_recent_apps(limit=10, host="localhost", port=18080):
    """List recent applications from History Server."""
    apps_data = _get_from_history("/api/v1/applications", host, port)
    if not apps_data:
        return []
    return apps_data[:limit]


# ═══════════════════════════════════════════════════════════════
#  METRICS & FORMATTING (shared with sparkmonitor.py)
# ═══════════════════════════════════════════════════════════════

def _fmt_bytes(b):
    if not b:
        return "0 B"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} PB"


def _fmt_ms(ms):
    if not ms:
        return "0 ms"
    if ms < 1000:   return f"{ms:.0f} ms"
    if ms < 60_000: return f"{ms/1000:.1f} s"
    return f"{ms/60_000:.1f} min"


def _fmt_elapsed(s):
    if s < 60:  return f"{s:.1f} s"
    return f"{int(s//60)} min {int(s%60)} s"


# ═══════════════════════════════════════════════════════════════
#  ADVICE ENGINE (copied from sparkmonitor.py)
# ═══════════════════════════════════════════════════════════════

_ADVICE = [
    dict(
        trigger = lambda t, e: t["diskBytesSpilled"] > 0,
        level   = "error",
        title   = "Your data spilled to disk",
        what    = "Spark ran out of memory and had to write "
                  "{disk_spill} to the cluster's hard drive to continue.",
        why     = "Reading from disk is 10-100x slower than reading from memory. "
                  "This is one of the most common reasons a job takes much longer than expected. "
                  "If enough data spills, the job can crash entirely.",
        fix     = "Add .cache() on the DataFrame you reuse most, right after you build it. "
                  "This keeps it in memory so Spark never needs to recompute or reload it.",
        code    = "# Find the DataFrame used in multiple cells and cache it:\n"
                  "df = df.cache()\n"
                  "df.count()  # this line forces the cache to load now\n\n"
                  "# From this point, all .show(), .count(), .groupBy() on df\n"
                  "# will read from memory — not from S3 again.",
    ),

    dict(
        trigger = lambda t, e: t["memoryBytesSpilled"] > 0 and t["diskBytesSpilled"] == 0,
        level   = "warn",
        title   = "Memory pressure — close to spilling",
        what    = "Spark shuffled {mem_spill} of data between memory pools "
                  "because executors were running low on RAM.",
        why     = "This is a warning sign. Your job is close to the edge. "
                  "If you add more data or remove a filter, the next run "
                  "will likely spill to disk and become much slower.",
        fix     = "Cache the DataFrame you use most to reduce memory pressure, "
                  "or ask the platform team if executor memory can be increased.",
        code    = "df = df.cache()\n"
                  "df.count()  # materialise the cache now",
    ),

    dict(
        trigger = lambda t, e: 500 * 1024**2 < t["shuffleWriteBytes"] <= 1 * 1024**3,
        level   = "warn",
        title   = "Large shuffle — join or groupBy moved a lot of data",
        what    = "Spark moved {shuffle_write} of data across the network "
                  "between cluster nodes to complete a join or groupBy.",
        why     = "Network transfers are expensive. Every worker has to wait "
                  "for data to arrive before it can continue. "
                  "The bigger the shuffle, the longer everything waits.",
        fix     = "If you are joining a large table to a small one, "
                  "use broadcast() to send the small table to every node. "
                  "This eliminates the shuffle entirely.",
        code    = "from pyspark.sql.functions import broadcast\n\n"
                  "# Before (causes a big shuffle):\n"
                  "result = big_df.join(small_df, 'key')\n\n"
                  "# After (no shuffle — small_df is sent to every node):\n"
                  "result = big_df.join(broadcast(small_df), 'key')\n\n"
                  "# Use broadcast() when the small table is under ~100 MB.",
    ),

    dict(
        trigger = lambda t, e: t["shuffleWriteBytes"] > 1 * 1024**3,
        level   = "error",
        title   = "Very large shuffle — this is likely your main bottleneck",
        what    = "Spark moved {shuffle_write} of data across the network. "
                  "At this scale the shuffle is almost certainly why the job is slow.",
        why     = "Every executor has to send data to every other executor. "
                  "Nothing can proceed until all transfers are complete. "
                  "At 1 GB+ this dominates total runtime.",
        fix     = "Option 1 (best): If one table is small, use broadcast(). "
                  "Option 2: Repartition both tables on the join key before joining — "
                  "this ensures matching keys land on the same node and avoids a full cross-cluster shuffle.",
        code    = "# Option 1 — use broadcast() if small_df is under ~100 MB:\n"
                  "from pyspark.sql.functions import broadcast\n"
                  "result = big_df.join(broadcast(small_df), 'join_key')\n\n"
                  "# Option 2 — repartition on the join key before joining:\n"
                  "df1 = df1.repartition(200, 'join_key')\n"
                  "df2 = df2.repartition(200, 'join_key')\n"
                  "result = df1.join(df2, 'join_key')",
    ),

    dict(
        trigger = lambda t, e: t["numTasks"] < 10 and t["inputBytes"] > 100 * 1024**2,
        level   = "warn",
        title   = "Work is barely parallelised — cluster is mostly idle",
        what    = "Spark used only {num_tasks} parallel tasks to process "
                  "{input_bytes} of data.",
        why     = "Your cluster has many cores, but only {num_tasks} are doing any work. "
                  "The rest are sitting idle. Your job is running almost single-threaded, "
                  "which makes it far slower than it needs to be.",
        fix     = "Add .repartition() right after reading your data. "
                  "This tells Spark to split the data into more pieces so "
                  "more of the cluster can work in parallel.",
        code    = "# Add this immediately after spark.read:\n"
                  "df = spark.read.parquet('s3://your-bucket/data/')\n"
                  "df = df.repartition(200)   # 200 is a safe starting point\n\n"
                  "# Then continue with your filters, joins, groupBys as normal.\n"
                  "# The cluster will now use many more cores in parallel.",
    ),

    dict(
        trigger = lambda t, e: (
            t["executorRunTime"] > 0
            and t["jvmGcTime"] / t["executorRunTime"] > 0.10
        ),
        level   = "warn",
        title   = "High garbage collection time — Python UDFs suspected",
        what    = "The JVM spent {gc_pct}% of executor time cleaning up memory "
                  "({gc_time}), not running your computation.",
        why     = "Python UDFs (functions you write yourself with @udf) force Spark to "
                  "pass data between Java and Python constantly, creating thousands of "
                  "short-lived objects that the JVM must then clean up. "
                  "This steals time from your actual job.",
        fix     = "Replace Python UDFs with Spark's built-in SQL functions where possible. "
                  "Built-in functions run entirely inside the JVM and have no Python overhead.",
        code    = "# SLOW — Python UDF:\n"
                  "from pyspark.sql.functions import udf\n"
                  "from pyspark.sql.types import StringType\n"
                  "my_upper = udf(lambda x: x.upper(), StringType())\n"
                  "df = df.withColumn('name_upper', my_upper('name'))\n\n"
                  "# FAST — built-in function (same result, no Python overhead):\n"
                  "from pyspark.sql.functions import upper\n"
                  "df = df.withColumn('name_upper', upper('name'))\n\n"
                  "# Other common built-ins: lower, trim, split, regexp_replace,\n"
                  "# to_date, datediff, coalesce, when, col — check the Spark docs.",
    ),
]


# ═══════════════════════════════════════════════════════════════
#  HTML BUILDING BLOCKS
# ═══════════════════════════════════════════════════════════════

_MONO = "font-family:'Courier New',Courier,monospace"

_THEME = {
    "error": {
        "bg": "#FFF0F0", "bar": "#C0392B",
        "head": "#7B1A1A", "icon": "🔴",
        "label_bg": "#FDDEDE", "label_fg": "#7B1A1A"
    },
    "warn": {
        "bg": "#FFF8E6", "bar": "#E8A000",
        "head": "#7A4900", "icon": "🟡",
        "label_bg": "#FFF0C0", "label_fg": "#7A4900"
    },
    "ok": {
        "bg": "#F3FAF0", "bar": "#2E7D32",
        "head": "#1B5E20", "icon": "🟢",
        "label_bg": "#D8F0D4", "label_fg": "#1B5E20"
    },
}


def _advice_card(level, title, what, why, fix, code):
    th = _THEME[level]
    code_html = ""
    if code:
        esc = code.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
        code_html = f"""
        <div style="margin-top:10px">
          <div style="font-size:10px;font-weight:700;color:{th['bar']};
                      text-transform:uppercase;letter-spacing:.07em;margin-bottom:3px">
            Copy this fix into your notebook:
          </div>
          <pre style="margin:0;padding:9px 12px;background:#1C1917;color:#6EE7B7;
                      font-size:11px;border-radius:5px;line-height:1.6;
                      overflow-x:auto;white-space:pre-wrap">{esc}</pre>
        </div>"""

    return f"""
    <div style="margin-top:12px;border-left:4px solid {th['bar']};
                border-radius:0 8px 8px 0;background:{th['bg']};
                padding:12px 16px;font-family:Arial,sans-serif">
      <div style="font-size:13px;font-weight:700;color:{th['head']};margin-bottom:8px">
        {th['icon']}&nbsp; {title}
      </div>
      <table style="border-collapse:collapse;width:100%;font-size:12px;color:#333">
        <tr>
          <td style="vertical-align:top;padding:3px 10px 3px 0;width:110px">
            <span style="background:{th['label_bg']};color:{th['label_fg']};
                         font-size:10px;font-weight:700;padding:2px 6px;
                         border-radius:3px;text-transform:uppercase;
                         letter-spacing:.05em;white-space:nowrap">What happened</span>
          </td>
          <td style="vertical-align:top;padding:3px 0;line-height:1.5">{what}</td>
        </tr>
        <tr>
          <td style="vertical-align:top;padding:6px 10px 3px 0">
            <span style="background:{th['label_bg']};color:{th['label_fg']};
                         font-size:10px;font-weight:700;padding:2px 6px;
                         border-radius:3px;text-transform:uppercase;
                         letter-spacing:.05em;white-space:nowrap">Why it matters</span>
          </td>
          <td style="vertical-align:top;padding:6px 0 3px 0;line-height:1.5">{why}</td>
        </tr>
        <tr>
          <td style="vertical-align:top;padding:6px 10px 3px 0">
            <span style="background:{th['label_bg']};color:{th['label_fg']};
                         font-size:10px;font-weight:700;padding:2px 6px;
                         border-radius:3px;text-transform:uppercase;
                         letter-spacing:.05em;white-space:nowrap">What to do</span>
          </td>
          <td style="vertical-align:top;padding:6px 0 3px 0;line-height:1.5">{fix}</td>
        </tr>
      </table>
      {code_html}
    </div>"""


def _metric_row(label, tooltip, value_str, highlight=False):
    bg  = "background:#FFF8E6;" if highlight else "background:#fff;"
    dot = " ⚠" if highlight else ""
    return f"""
    <tr title="{tooltip}">
      <td style="padding:5px 16px 5px 12px;font-size:12px;
                 color:#555;{bg}border-bottom:1px solid #eee">{label}{dot}</td>
      <td style="padding:5px 12px;font-size:12px;{_MONO};{bg}
                 color:#1C1917;font-weight:{'600' if highlight else '400'};
                 border-bottom:1px solid #eee">{value_str}</td>
    </tr>"""


# ═══════════════════════════════════════════════════════════════
#  MAIN ANALYSIS FUNCTION
# ═══════════════════════════════════════════════════════════════

def _render_job_report(app_id, stages, app_info):
    """Render analysis report for a completed job."""

    if not stages:
        display(HTML(f"""
        <div style="font-family:Arial,sans-serif;max-width:640px;margin:8px 0;
                    border-left:4px solid #E8A000;border-radius:0 8px 8px 0;
                    background:#FFF8E6;padding:12px 16px">
          <div style="font-size:13px;font-weight:700;color:#7A4900;margin-bottom:8px">
            🟡 No stage data found
          </div>
          <div style="font-size:12px;color:#333;line-height:1.7">
            The application <code>{app_id}</code> either completed without running
            any Spark stages, or the History Server data is not available yet.
          </div>
        </div>"""))
        return

    # Aggregate totals
    t = {
        "numTasks":           sum(s.get("numTasks", 0)             for s in stages),
        "executorRunTime":    sum(s.get("executorRunTime", 0)      for s in stages),
        "executorCpuTime":    sum(s.get("executorCpuTime", 0)      for s in stages),
        "jvmGcTime":          sum(s.get("jvmGcTime", 0)            for s in stages),
        "shuffleReadBytes":   sum(s.get("shuffleReadBytes", 0)     for s in stages),
        "shuffleWriteBytes":  sum(s.get("shuffleWriteBytes", 0)    for s in stages),
        "diskBytesSpilled":   sum(s.get("diskBytesSpilled", 0)     for s in stages),
        "memoryBytesSpilled": sum(s.get("memoryBytesSpilled", 0)   for s in stages),
        "inputBytes":         sum(s.get("inputBytes", 0)           for s in stages),
        "outputBytes":        sum(s.get("outputBytes", 0)          for s in stages),
        "peakExecutionMemory":max((s.get("peakExecutionMemory", 0) for s in stages), default=0),
    }

    # Calculate elapsed time from app info if available
    elapsed_s = 0
    if app_info:
        submit_time = app_info.get("timestamp", 0)
        end_time = app_info.get("timestamp", 0)
        if "duration" in app_info:
            elapsed_s = app_info["duration"] / 1000.0
        elif submit_time and end_time:
            elapsed_s = (end_time - submit_time) / 1000.0

    gc_pct = int(100 * t["jvmGcTime"] / t["executorRunTime"]) \
             if t["executorRunTime"] > 0 else 0

    # Template substitutions for advice text
    subs = {
        "disk_spill":    _fmt_bytes(t["diskBytesSpilled"]),
        "mem_spill":     _fmt_bytes(t["memoryBytesSpilled"]),
        "shuffle_write": _fmt_bytes(t["shuffleWriteBytes"]),
        "num_tasks":     str(t["numTasks"]),
        "input_bytes":   _fmt_bytes(t["inputBytes"]),
        "gc_pct":        str(gc_pct),
        "gc_time":       _fmt_ms(t["jvmGcTime"]),
        "elapsed":       _fmt_elapsed(elapsed_s),
    }

    # Fire advice rules
    fired = []
    for rule in _ADVICE:
        try:
            if rule["trigger"](t, elapsed_s):
                fired.append(rule)
        except Exception:
            pass

    # Health banner
    has_error = any(r["level"] == "error" for r in fired)
    has_warn  = any(r["level"] == "warn"  for r in fired)
    if has_error:
        hth = _THEME["error"]
        htext = "Issues found — see the advice cards below"
    elif has_warn:
        hth = _THEME["warn"]
        htext = "Warnings found — review the advice cards below"
    else:
        hth = _THEME["ok"]
        htext = "Looks healthy — no major issues detected"

    # Metrics table
    spill_disk   = t["diskBytesSpilled"] > 0
    spill_mem    = t["memoryBytesSpilled"] > 0
    big_shuffle  = t["shuffleWriteBytes"] > 500 * 1024**2
    few_tasks    = t["numTasks"] < 10 and t["inputBytes"] > 100 * 1024**2
    high_gc      = gc_pct > 10

    metrics_html = "".join([
        _metric_row(
            "Total time",
            "Wall-clock time from job start to finish",
            _fmt_elapsed(elapsed_s)),
        _metric_row(
            "Parallel tasks used",
            "How many parallel pieces Spark split the work into. "
            "Healthy = 50 to 2000. Low on large data = cluster underused.",
            str(t["numTasks"]),
            highlight=few_tasks),
        _metric_row(
            "Data read from S3 / HDFS",
            "Total bytes Spark read from storage",
            _fmt_bytes(t["inputBytes"])),
        _metric_row(
            "Data written to S3 / HDFS",
            "Total bytes Spark wrote to storage",
            _fmt_bytes(t["outputBytes"])),
        _metric_row(
            "Network shuffle (join / groupBy cost)",
            "Data moved between cluster nodes. "
            "Zero = ideal. High = expensive join or groupBy.",
            _fmt_bytes(t["shuffleWriteBytes"]),
            highlight=big_shuffle),
        _metric_row(
            "Spilled to disk",
            "Data that didn't fit in executor memory and was written to disk. "
            "Any value above zero is a problem.",
            _fmt_bytes(t["diskBytesSpilled"]) if spill_disk else "None  ✓",
            highlight=spill_disk),
        _metric_row(
            "Spilled from memory (RAM pressure)",
            "Data shuffled between memory pools because RAM was running low. "
            "Warning sign before disk spill occurs.",
            _fmt_bytes(t["memoryBytesSpilled"]) if spill_mem else "None  ✓",
            highlight=spill_mem),
        _metric_row(
            "GC time (memory housekeeping)",
            "Time the JVM spent cleaning up objects. "
            "Above 10% of run time usually means Python UDFs are causing overhead.",
            f"{_fmt_ms(t['jvmGcTime'])}  ({gc_pct}% of run time)",
            highlight=high_gc),
        _metric_row(
            "Peak executor memory",
            "The most memory any single executor used at one time. "
            "If close to the executor memory limit, spill is likely on the next run.",
            _fmt_bytes(t["peakExecutionMemory"])),
    ])

    # Advice cards
    cards_html = ""
    for rule in fired:
        w = rule["what"].format(**subs)
        y = rule["why"].format(**subs)
        f = rule["fix"].format(**subs)
        cards_html += _advice_card(rule["level"], rule["title"], w, y, f, rule.get("code"))

    if not fired:
        cards_html = f"""
        <div style="margin-top:12px;border-left:4px solid {_THEME['ok']['bar']};
                    border-radius:0 8px 8px 0;background:{_THEME['ok']['bg']};
                    padding:12px 16px;font-size:12px;color:#1B5E20;
                    font-family:Arial,sans-serif;line-height:1.6">
          <strong>No issues detected.</strong> This job ran efficiently.<br>
          If the run time still feels slow, the data volume itself may be the
          limiting factor rather than how Spark is configured.
        </div>"""

    # Full report
    app_name = app_info.get("name", "Unknown") if app_info else "Unknown"
    display(HTML(f"""
    <div style="font-family:Arial,sans-serif;margin:10px 0;max-width:660px">

      <div style="background:#1C1917;color:#FDE68A;padding:8px 14px;
                  border-radius:8px 8px 0 0;font-size:13px;font-weight:700;
                  display:flex;justify-content:space-between;align-items:center">
        <span>📊 Job Analysis: {app_name}</span>
        <span style="font-weight:400;font-size:12px;opacity:.75">
          {len(stages)} stage{'s' if len(stages)!=1 else ''}
          &nbsp;·&nbsp; {t['numTasks']} tasks
          &nbsp;·&nbsp; {_fmt_elapsed(elapsed_s)}
        </span>
      </div>

      <div style="padding:8px 14px;background:{hth['bg']};border-left:4px solid {hth['bar']};
                  border-right:1px solid #ddd;font-size:13px;font-weight:700;
                  color:{hth['bar']}">
        {hth['icon']}&nbsp; {htext}
      </div>

      <details open style="border:1px solid #ddd;border-top:none">
        <summary style="padding:7px 14px;font-size:12px;font-weight:600;
                        color:#555;cursor:pointer;background:#F9F9F7;
                        user-select:none">
          ▸ Full metrics (click to collapse)
        </summary>
        <table style="border-collapse:collapse;width:100%">
          {metrics_html}
        </table>
      </details>

      {cards_html}

      <div style="margin-top:8px;font-size:10px;color:#aaa;text-align:right">
        App ID: <code>{app_id}</code> · analyze_job · reads Spark History Server
      </div>

    </div>"""))


def analyze_job(app_id=None, host="localhost", port=18080):
    """
    Analyze a completed Spark job from History Server.

    Parameters
    ----------
    app_id : str, optional
        Application ID to analyze (e.g., "app-20240115-0001").
        If None, shows recent jobs and lets you choose.
    host : str
        History Server host (default: localhost)
    port : int
        History Server port (default: 18080)

    Examples
    --------
    >>> analyze_job("app-20240115-0001")  # Analyze specific app
    >>> analyze_job()  # Show recent jobs
    """

    # If no app_id, show recent jobs
    if not app_id:
        apps = _list_recent_apps(limit=10, host=host, port=port)
        if not apps:
            display(HTML("""
            <div style="font-family:Arial,sans-serif;max-width:560px;padding:12px 16px;
                        background:#FFF0F0;border-left:4px solid #C0392B;
                        border-radius:0 8px 8px 0;font-size:13px;color:#7B1A1A">
              <strong>Could not connect to Spark History Server.</strong><br>
              <span style="font-size:12px;line-height:1.7">
              Make sure History Server is running at <code>http://{host}:{port}</code>
              </span>
            </div>"""))
            return

        # Display list of recent apps
        app_rows = ""
        for app in apps:
            app_id_val = app.get("id", "Unknown")
            app_name = app.get("name", "Unknown")
            app_status = app.get("status", "Unknown")
            app_rows += f"""
            <tr>
              <td style="padding:8px 12px;font-size:12px;border-bottom:1px solid #eee">{app_name}</td>
              <td style="padding:8px 12px;font-size:12px;border-bottom:1px solid #eee;font-family:monospace">{app_id_val}</td>
              <td style="padding:8px 12px;font-size:12px;border-bottom:1px solid #eee;color:#666">{app_status}</td>
            </tr>"""

        display(HTML(f"""
        <div style="font-family:Arial,sans-serif;max-width:700px;margin:10px 0">
          <div style="background:#1C1917;color:#FDE68A;padding:9px 14px;
                      border-radius:8px 8px 0 0;font-size:13px;font-weight:700">
            📊 Recent Applications
          </div>
          <table style="border-collapse:collapse;width:100%;border:1px solid #ddd;border-top:none">
            <thead>
              <tr style="background:#f5f5f5">
                <th style="padding:8px 12px;text-align:left;font-size:12px;font-weight:600;border-bottom:1px solid #ddd">Name</th>
                <th style="padding:8px 12px;text-align:left;font-size:12px;font-weight:600;border-bottom:1px solid #ddd">App ID</th>
                <th style="padding:8px 12px;text-align:left;font-size:12px;font-weight:600;border-bottom:1px solid #ddd">Status</th>
              </tr>
            </thead>
            <tbody>
              {app_rows}
            </tbody>
          </table>
          <div style="margin-top:12px;padding:12px;background:#f9f9f9;border:1px solid #ddd;border-radius:4px;font-size:12px">
            <strong>To analyze a job, run:</strong><br>
            <code style="background:#fff;padding:4px 8px;border-radius:3px;font-family:monospace">analyze_job("app-20240115-0001")</code>
          </div>
        </div>"""))
        return

    # Analyze specific app
    app_info = _get_app_info(app_id, host, port)
    if not app_info:
        display(HTML(f"""
        <div style="font-family:Arial,sans-serif;max-width:560px;padding:12px 16px;
                    background:#FFF0F0;border-left:4px solid #C0392B;
                    border-radius:0 8px 8px 0;font-size:13px;color:#7B1A1A">
          <strong>Could not find application "{app_id}".</strong><br>
          <span style="font-size:12px;line-height:1.7">
          Make sure the app ID is correct and the History Server is running.
          </span>
        </div>"""))
        return

    stages = _get_app_stages(app_id, host, port)
    _render_job_report(app_id, stages, app_info)


# ═══════════════════════════════════════════════════════════════
#  COMMAND LINE INTERFACE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze completed Spark jobs from History Server")
    parser.add_argument(
        "--app-id", "-a",
        help="Application ID to analyze (e.g., app-20240115-0001)")
    parser.add_argument(
        "--recent", "-r",
        action="store_true",
        help="Show recent applications")
    parser.add_argument(
        "--host",
        default="localhost",
        help="History Server host (default: localhost)")
    parser.add_argument(
        "--port",
        type=int,
        default=18080,
        help="History Server port (default: 18080)")

    args = parser.parse_args()

    if args.app_id:
        analyze_job(args.app_id, args.host, args.port)
    elif args.recent:
        analyze_job(None, args.host, args.port)
    else:
        parser.print_help()
