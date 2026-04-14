"""
sparkmonitor.py  —  Spark performance monitor for Jupyter on EMR
────────────────────────────────────────────────────────────────
Zero-dependency: uses Spark's built-in REST API (port 4040).
No pip install, no JAR, no cluster changes needed.

HOW TO USE
──────────
1. Add this as Cell 1 of your notebook:

       %run s3://your-bucket/tools/sparkmonitor.py

2. Put  %%measure  at the top of any cell you want to profile:

       %%measure
       df = spark.read.parquet("s3://...")
       df.groupBy("region").count().show()

3. Read the report that appears after the cell runs.
   Any yellow or red rows include a plain-English fix you can copy.
"""

import json
import time
import urllib.request
from IPython.core.magic import register_cell_magic
from IPython.display import display, HTML

# ═══════════════════════════════════════════════════════════════
#  SECTION 1: SPARK CONTEXT DETECTION
#  ═════════════════════════════════════════════════════════════
#  Finds the user's SparkContext, app ID, and cluster info.
#  Uses three strategies: globals(), IPython user namespace.
# ═══════════════════════════════════════════════════════════════

def _get_spark_context_info():
    """Extract Spark configuration from sparkContext.

    Tries multiple ways to get the SparkContext:
    1. Check globals() for 'spark' variable (Jupyter kernel global)
    2. Check globals() for 'sc' variable (older Spark shells)
    3. Try IPython kernel to access user namespace
    """
    try:
        sc = None

        # Try globals() first (works in Jupyter kernel)
        globs = globals()
        if 'spark' in globs:
            try:
                sc = globs['spark'].sparkContext
            except Exception:
                pass

        if not sc and 'sc' in globs:
            try:
                sc = globs['sc']
            except Exception:
                pass

        # Try IPython kernel access (if in Jupyter)
        if not sc:
            try:
                from IPython import get_ipython
                ipython = get_ipython()
                if ipython:
                    user_ns = ipython.user_ns
                    if 'spark' in user_ns:
                        sc = user_ns['spark'].sparkContext
                    elif 'sc' in user_ns:
                        sc = user_ns['sc']
            except Exception:
                pass

        if not sc:
            return None, None, None

        conf = sc.getConf()
        app_id = sc.applicationId

        if not app_id:
            return None, None, None

        # Get YARN RM host from config
        yarn_rm_host = conf.get("spark.yarn.resourceManager.hostname")
        if not yarn_rm_host:
            master = conf.get("spark.master", "")
            if "yarn" in master.lower():
                yarn_rm_host = "localhost"

        return app_id, yarn_rm_host, conf
    except Exception:
        pass

    return None, None, None


def _try_get_sparkcontext():
    """Try to get the user's SparkContext object.

    Searches in order:
    1. globals()['spark'].sparkContext (Jupyter with SparkSession)
    2. globals()['sc'] (older Spark shells)
    3. IPython user namespace (Jupyter kernel user variables)

    Returns SparkContext or None if not found.
    """
    try:
        # Try globals() first (works in Jupyter kernel)
        globs = globals()
        if 'spark' in globs:
            try:
                return globs['spark'].sparkContext
            except Exception:
                pass

        if 'sc' in globs:
            try:
                return globs['sc']
            except Exception:
                pass

        # Try IPython kernel access (if in Jupyter)
        try:
            from IPython import get_ipython
            ipython = get_ipython()
            if ipython:
                user_ns = ipython.user_ns
                if 'spark' in user_ns:
                    return user_ns['spark'].sparkContext
                elif 'sc' in user_ns:
                    return user_ns['sc']
        except Exception:
            pass
    except Exception:
        pass

    return None


def _get_yarn_spark_ui(app_id, yarn_rm_host):
    """Get Spark UI info from YARN Resource Manager (for running apps)."""
    if not app_id or not yarn_rm_host:
        return None, None

    try:
        yarn_rm_port = 8088  # YARN RM default port
        yarn_url = f"http://{yarn_rm_host}:{yarn_rm_port}/ws/v1/cluster/apps/{app_id}"
        with urllib.request.urlopen(yarn_url, timeout=5) as r:
            app_data = json.loads(r.read())
            tracking_url = app_data.get("app", {}).get("trackingUrl", "")
            if tracking_url and tracking_url != "UNDEFINED":
                # Parse the tracking URL (should be Spark UI URL)
                from urllib.parse import urlparse
                parsed = urlparse(tracking_url)
                host = parsed.hostname or 'localhost'
                port = parsed.port or 4040
                return host, port
    except Exception:
        pass

    return None, None


def _get_history_server_info(app_id):
    """Get Spark info from History Server (for completed or current apps).

    History Server is stable and always available, even after driver exits.
    """
    if not app_id:
        return None, None

    # Try standard History Server location
    history_host = "localhost"
    history_port = 18080

    try:
        history_url = f"http://{history_host}:{history_port}/api/v1/applications/{app_id}"
        with urllib.request.urlopen(history_url, timeout=5) as r:
            app_data = json.loads(r.read())
            if app_data:
                # History Server is accessible
                return history_host, history_port
    except Exception:
        pass

    return None, None


def _find_spark_port_and_host():
    """Find Spark UI port and host using multiple strategies.

    Tries in priority order:
    1. Spark History Server (port 18080) — most stable, works for live & completed jobs
    2. YARN Resource Manager (port 8088) — queries for driver location
    3. Direct Spark UI (sc.getWebUI()) — if port is accessible
    4. Localhost scanning (ports 4040-4045) — last resort

    On EMR, History Server is usually the only reliable option because
    port 4040 is often blocked by security groups.
    """
    app_id, yarn_rm_host, conf = _get_spark_context_info()

    # Strategy 1: History Server (most reliable)
    if app_id:
        host, port = _get_history_server_info(app_id)
        if host and port:
            return host, port

    # Strategy 2: YARN Resource Manager (good for EMR)
    if app_id and yarn_rm_host:
        host, port = _get_yarn_spark_ui(app_id, yarn_rm_host)
        if host and port:
            return host, port

    # Strategy 3: Direct SparkContext.getWebUI()
    try:
        sc = _try_get_sparkcontext()
        if sc:
            try:
                ui_url = sc._jsc.sc().getWebUI().get()
                if ui_url:
                    from urllib.parse import urlparse
                    parsed = urlparse(ui_url)
                    host = parsed.hostname or 'localhost'
                    port = parsed.port or 4040
                    return host, port
            except Exception:
                pass

            # Fallback from Spark config
            try:
                driver_host = conf.get("spark.driver.host", "localhost") if conf else "localhost"
                driver_port = conf.get("spark.ui.port", "4040") if conf else "4040"
                return driver_host, int(driver_port)
            except Exception:
                pass
    except Exception:
        pass

    # Strategy 4: Localhost port scanning (unlikely to work on EMR)
    for port in range(4040, 4046):
        try:
            urllib.request.urlopen(
                f"http://localhost:{port}/api/v1/applications", timeout=2)
            return "localhost", port
        except Exception:
            pass

    return None, None


def _get(path, host, port):
    """Generic HTTP GET for Spark REST API.

    Args:
        path: REST endpoint (e.g., "/api/v1/applications")
        host: Hostname or IP
        port: Port number

    Returns:
        Parsed JSON dict, or None if request fails
    """
    if not host or not port:
        return None
    try:
        with urllib.request.urlopen(
                f"http://{host}:{port}{path}", timeout=5) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _get_app_id(host, port, sc=None):
    """Get Spark application ID.

    Tries in order:
    1. Direct SparkContext.applicationId (most reliable)
    2. REST API query (fallback)
    """
    # Try to get directly from sparkContext
    if sc is None:
        sc = _try_get_sparkcontext()

    if sc:
        try:
            app_id = sc.applicationId
            if app_id:
                return app_id
        except Exception:
            pass

    # Fallback: query Spark REST API for list of apps
    apps = _get("/api/v1/applications", host, port)
    return apps[0]["id"] if apps else None


def _stage_ids_done(host, port, app_id):
    """Get set of all COMPLETE stage IDs for this app.

    Used to capture a "before" snapshot before user's cell runs.
    """
    stages = _get(f"/api/v1/applications/{app_id}/stages", host, port) or []
    return {s["stageId"] for s in stages if s.get("status") == "COMPLETE"}


def _new_stages(host, port, app_id, before):
    """Get list of NEW stages (COMPLETE or FAILED) that appeared after 'before' snapshot.

    Filters to only stages that:
    1. Are in COMPLETE or FAILED status (terminal states, not RUNNING)
    2. Were not in the 'before' snapshot

    These are the stages we care about (the ones that just ran).
    Including FAILED stages lets users see which stages failed, not just which succeeded.
    """
    stages = _get(f"/api/v1/applications/{app_id}/stages", host, port) or []
    return [s for s in stages
            if s.get("status") in ["COMPLETE", "FAILED"] and s["stageId"] not in before]


def _get_all_stages(host, port, app_id):
    """Get ALL stages including RUNNING/FAILED (for debugging/logging).

    Unlike _new_stages(), this includes incomplete stages.
    Used to check if stages are still being indexed by History Server.
    """
    stages = _get(f"/api/v1/applications/{app_id}/stages", host, port) or []
    return stages


def _capture_stages_with_retry(host, port, app_id, before, elapsed):
    """Intelligently retry stage capture with adaptive delays.

    Problem: Spark History Server reads event logs from S3, which has latency.
    For quick jobs, HS hasn't even started indexing yet. For long jobs, HS
    is likely already indexed.

    Solution: Vary retry strategy based on job elapsed time:
    - Very quick job (< 0.5s): HS not started, wait 3.0s initial, 8 retries
    - Quick job (0.5-2.0s): HS catching up, wait 2.0s initial, 6 retries
    - Normal job (2.0-5.0s): HS mostly caught up, wait 1.0s initial, 5 retries
    - Long job (> 5.0s): HS definitely caught up, wait 0.8s initial, 2 retries

    Global timeout: Never wait more than 15 seconds total.

    Args:
        host, port, app_id: Spark connection info
        before: Set of stage IDs that were complete before cell ran
        elapsed: How long the cell took to execute (in seconds)

    Returns:
        List of new COMPLETE stages, or [] if none found within timeout
    """
    # Determine retry strategy based on job duration
    if elapsed < 0.5:
        # Very quick job - History Server likely hasn't indexed yet
        initial_wait = 3.0      # Increased from 1.5
        max_retries = 8         # Increased from 5
        retry_interval = 1.5    # Increased from 1.0
    elif elapsed < 2.0:
        # Quick job
        initial_wait = 2.0      # Increased from 1.0
        max_retries = 6         # Increased from 4
        retry_interval = 1.0    # Increased from 0.8
    elif elapsed < 5.0:
        # Normal job
        initial_wait = 1.0      # Increased from 0.6
        max_retries = 5         # Increased from 3
        retry_interval = 0.8    # Increased from 0.6
    else:
        # Long job - lag is negligible
        initial_wait = 0.8      # Increased from 0.6
        max_retries = 2         # Increased from 1
        retry_interval = 0.6    # Same

    # First attempt
    time.sleep(initial_wait)
    new = _new_stages(host, port, app_id, before)
    all_stages = _get_all_stages(host, port, app_id)

    # DEBUG: Check if ANY stages exist
    if not all_stages and not new:
        # No stages at all - likely event logging issue
        return []

    if new:
        return new  # Found complete stages immediately, return

    # Retry loop for History Server catching up
    total_wait = initial_wait
    max_wait = 180.0  # Hard timeout: Never wait more than 3 minutes (180 seconds) total
                      # This is the absolute maximum time we'll wait for all new stages to complete
                      # If stages don't complete in 3 minutes, we give up and return what we have

    for attempt in range(1, max_retries + 1):
        # Check if we've exceeded maximum wait time
        if total_wait >= max_wait:
            break

        # Wait with slight backoff
        wait = min(retry_interval, max_wait - total_wait)
        time.sleep(wait)
        total_wait += wait

        # Try again
        new = _new_stages(host, port, app_id, before)
        all_stages = _get_all_stages(host, port, app_id)

        if new:
            return new  # Found complete stages!

        # If we found ANY stages but none are COMPLETE, stages are still running
        if all_stages and not new:
            # Stages exist but not complete yet - keep waiting
            continue

    # No stages found after retries
    # Timeout reached (hard limit: 3 minutes)
    # This is normal for very quick jobs or if using History Server on S3
    # If job is still running after 3 minutes, stages may appear later
    return []


# ═══════════════════════════════════════════════════════════════
#  INTERNAL — Formatters
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
#  ADVICE ENGINE
#  Each entry is one issue the modeller might see.
#
#  trigger  — lambda(totals_dict, elapsed_seconds) → bool
#  level    — "warn" or "error"
#  title    — short heading in plain English
#  what     — what happened (one sentence, may use {subs})
#  why      — why it slows things down or risks the cluster
#  fix      — what the modeller should do (plain text)
#  code     — copy-paste code snippet shown in a dark code box
#
# ═══════════════════════════════════════════════════════════════
#  PERFORMANCE THRESHOLDS
# ═══════════════════════════════════════════════════════════════
#
#  Shuffle (Network Transfer) Thresholds:
#    0 - 500 MB:   ✓ Healthy (no warning)
#    500 MB - 1 GB: ⚠️ Warning (getting expensive, consider broadcast/repartition)
#    > 1 GB:       ❌ Error (critical bottleneck, almost certainly your problem)
#
#  Why these thresholds?
#    - 500 MB = Starting to notice network delay in typical clusters
#    - 1 GB = Shuffle dominates total runtime, all executors wait for network
#    - Examples: 100GB data → 100 partitions = 1GB per shuffle = CRITICAL
#
#  Byte Conversions:
#    1024 bytes       = 1 KB
#    1024**2 bytes    = 1 MB  (1,048,576 bytes)
#    1024**3 bytes    = 1 GB  (1,073,741,824 bytes)
#    1024**4 bytes    = 1 TB  (1,099,511,627,776 bytes)
#
# ═══════════════════════════════════════════════════════════════

_ADVICE = [

    dict(
        trigger = lambda t, e: t["diskBytesSpilled"] > 0,
        level   = "error",
        title   = "Your data spilled to disk ({max_spill_stage})",
        what    = "Spark ran out of memory in {max_spill_stage} and had to write "
                  "{max_spill_bytes} to the cluster's hard drive to continue.",
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
        # Shuffle warning threshold: 500MB to 1GB
        # 500 * 1024**2 = 524,288,000 bytes = 500 MB (lower limit, warn starts here)
        # 1 * 1024**3   = 1,073,741,824 bytes = 1 GB (upper limit, becomes error)
        # Network transfers above 500MB are expensive but manageable.
        # Above 1GB, shuffle usually dominates total runtime.
        trigger = lambda t, e: 500 * 1024**2 < t["shuffleWriteBytes"] <= 1 * 1024**3,
        level   = "warn",
        title   = "Large shuffle in {max_shuffle_stage} — join or groupBy moved a lot of data",
        what    = "Spark moved {max_shuffle_bytes} of data across the network in {max_shuffle_stage} "
                  "to complete a join or groupBy.",
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
        # Shuffle error threshold: > 1GB
        # 1 * 1024**3 = 1,073,741,824 bytes = 1 GB
        # At this scale, shuffle becomes the dominant cost and limits parallelism.
        # All executors must wait for network transfers to complete before proceeding.
        # This is almost always the top reason for slow jobs at this scale.
        trigger = lambda t, e: t["shuffleWriteBytes"] > 1 * 1024**3,
        level   = "error",
        title   = "Very large shuffle in {max_shuffle_stage} — this is your main bottleneck",
        what    = "Spark moved {max_shuffle_bytes} of data across the network in {max_shuffle_stage}. "
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

    dict(
        trigger = lambda t, e: (
            e > 30
            and t["shuffleWriteBytes"] == 0
            and t["inputBytes"] == 0
        ),
        level   = "warn",
        title   = "Slow cell with no data reads — possible uncached lineage replay",
        what    = "This cell took {elapsed} but Spark did not read any data from storage.",
        why     = "Spark is lazy — it remembers the steps to build a DataFrame "
                  "but doesn't actually run them until you call an action like .show() or .count(). "
                  "If a previous cell defined a DataFrame without caching it, "
                  "Spark may be re-running all those steps from scratch every time.",
        fix     = "Go back to the cell where your main DataFrame was first built. "
                  "Add .cache() there and call .count() to force it to load. "
                  "Every cell that uses that DataFrame afterwards will be instant.",
        code    = "# In the cell where your DataFrame is first built:\n"
                  "df = (\n"
                  "    spark.read.parquet('s3://your-bucket/data/')\n"
                  "    .filter(df.year == 2024)\n"
                  "    .groupBy('region')\n"
                  "    .agg({'revenue': 'sum'})\n"
                  ")\n"
                  "df = df.cache()   # <-- add this line\n"
                  "df.count()        # <-- triggers the cache to load\n\n"
                  "# Now every subsequent cell that uses df will be fast\n"
                  "# because Spark reads from memory, not from S3 again.",
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


def _per_stage_breakdown(stages):
    """Generate HTML table showing key metrics per stage.

    Since EMR processes stage-by-stage, showing per-stage metrics helps identify
    which stage is the actual bottleneck.
    """
    if not stages:
        return ""

    rows = []
    rows.append("""
    <table style="border-collapse:collapse;width:100%;font-size:11px;margin:0">
      <thead>
        <tr style="background:#F9F9F7;border-bottom:1px solid #ddd">
          <th style="padding:6px 12px;text-align:left;font-weight:600;color:#555;border-right:1px solid #ddd">Stage</th>
          <th style="padding:6px 12px;text-align:right;font-weight:600;color:#555;border-right:1px solid #ddd">Tasks</th>
          <th style="padding:6px 12px;text-align:right;font-weight:600;color:#555;border-right:1px solid #ddd">Executor Time</th>
          <th style="padding:6px 12px;text-align:right;font-weight:600;color:#555;border-right:1px solid #ddd">Shuffle Write</th>
          <th style="padding:6px 12px;text-align:right;font-weight:600;color:#555;border-right:1px solid #ddd">Disk Spill</th>
          <th style="padding:6px 12px;text-align:right;font-weight:600;color:#555">Status</th>
        </tr>
      </thead>
      <tbody>
    """)

    for stage in sorted(stages, key=lambda s: s.get("stageId", 0)):
        stage_id = stage.get("stageId", "?")
        num_tasks = stage.get("numTasks", 0)
        executor_time = stage.get("executorRunTime", 0)
        shuffle_write = stage.get("shuffleWriteBytes", 0)
        disk_spill = stage.get("diskBytesSpilled", 0)
        status = stage.get("status", "?")

        # Highlight stages with issues
        shuffle_issue = shuffle_write > 500 * 1024**2
        spill_issue = disk_spill > 0
        row_bg = ""
        if spill_issue:
            row_bg = "background:#FFE6E6;"  # Light red for spill
        elif shuffle_issue:
            row_bg = "background:#FFF8E6;"  # Light yellow for shuffle
        else:
            row_bg = "background:#fff;"

        status_icon = "✓" if status == "COMPLETE" else "⚠" if status == "RUNNING" else "✗"

        rows.append(f"""
        <tr style="{row_bg}border-bottom:1px solid #eee">
          <td style="padding:5px 12px;border-right:1px solid #ddd;color:#1C1917;font-weight:600">Stage {stage_id}</td>
          <td style="padding:5px 12px;border-right:1px solid #ddd;color:#666;text-align:right">{num_tasks}</td>
          <td style="padding:5px 12px;border-right:1px solid #ddd;color:#666;text-align:right;{_MONO}">{_fmt_ms(executor_time)}</td>
          <td style="padding:5px 12px;border-right:1px solid #ddd;color:#{'#C0392B' if shuffle_issue else '#666'};text-align:right;{_MONO};font-weight:{'600' if shuffle_issue else '400'}">{_fmt_bytes(shuffle_write)}</td>
          <td style="padding:5px 12px;border-right:1px solid #ddd;color:#{'#C0392B' if spill_issue else '#666'};text-align:right;{_MONO};font-weight:{'600' if spill_issue else '400'}">{_fmt_bytes(disk_spill) if disk_spill > 0 else 'None'}</td>
          <td style="padding:5px 12px;text-align:right;color:#666">{status_icon} {status}</td>
        </tr>
        """)

    rows.append("""
      </tbody>
    </table>
    <div style="padding:8px 12px;font-size:11px;color:#666;border-top:1px solid #ddd;margin-top:4px">
      <strong>Note:</strong> EMR processes stages sequentially. A stage with high Shuffle Write or Disk Spill is a bottleneck.
      Use the metrics above to identify which stage needs optimization.
    </div>
    """)

    return "".join(rows)


# ═══════════════════════════════════════════════════════════════
#  MAIN REPORT RENDERER
# ═══════════════════════════════════════════════════════════════

def _find_problem_stages(stages):
    """Identify which stages have performance issues.

    Returns dict with:
    - max_shuffle_stage: (stage_id, bytes) for stage with largest shuffle
    - max_spill_stage: (stage_id, bytes) for stage with most disk spill
    - underutilized_stages: list of stage_ids with few tasks
    """
    problems = {
        "max_shuffle_stage": (None, 0),
        "max_spill_stage": (None, 0),
        "underutilized_stages": [],
    }

    for stage in stages:
        stage_id = stage.get("stageId")
        shuffle_bytes = stage.get("shuffleWriteBytes", 0)
        spill_bytes = stage.get("diskBytesSpilled", 0)
        num_tasks = stage.get("numTasks", 0)

        # Track stage with max shuffle
        if shuffle_bytes > problems["max_shuffle_stage"][1]:
            problems["max_shuffle_stage"] = (stage_id, shuffle_bytes)

        # Track stage with max spill
        if spill_bytes > problems["max_spill_stage"][1]:
            problems["max_spill_stage"] = (stage_id, spill_bytes)

        # Track underutilized stages
        if num_tasks < 5:
            problems["underutilized_stages"].append((stage_id, num_tasks))

    return problems


def _render_report(stages, elapsed_s, ml_libs=None):

    ml_libs = ml_libs or []

    # No Spark work ran
    if not stages:
        display(HTML(f"""
        <div style="font-family:Arial,sans-serif;max-width:640px;margin:8px 0;
                    border-left:4px solid #2E7D32;border-radius:0 8px 8px 0;
                    background:#F3FAF0;padding:12px 16px">
          <div style="font-size:13px;font-weight:700;color:#1B5E20;margin-bottom:8px">
            🟢 No Spark work ran in this cell
          </div>
          <div style="font-size:12px;color:#333;line-height:1.7">
            Your code defined a DataFrame but did not trigger any computation.<br><br>
            <strong>Spark is lazy</strong> — it plans the work but does nothing until
            you call an <strong>action</strong>. Actions are:<br>
            &nbsp; • <code>.show()</code> — display rows<br>
            &nbsp; • <code>.count()</code> — count rows<br>
            &nbsp; • <code>.collect()</code> — pull all rows to the driver (use with care)<br>
            &nbsp; • <code>.write.parquet(...)</code> — save to S3<br><br>
            Add one of the above to the end of your cell, then run it again.
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

    # Find problem stages for stage-specific advice
    problem_stages = _find_problem_stages(stages)

    gc_pct = int(100 * t["jvmGcTime"] / t["executorRunTime"]) \
             if t["executorRunTime"] > 0 else 0

    # Template substitutions for advice text
    max_shuffle_stage_id, max_shuffle_bytes = problem_stages["max_shuffle_stage"]
    max_spill_stage_id, max_spill_bytes = problem_stages["max_spill_stage"]

    subs = {
        "disk_spill":    _fmt_bytes(t["diskBytesSpilled"]),
        "mem_spill":     _fmt_bytes(t["memoryBytesSpilled"]),
        "shuffle_write": _fmt_bytes(t["shuffleWriteBytes"]),
        "num_tasks":     str(t["numTasks"]),
        "input_bytes":   _fmt_bytes(t["inputBytes"]),
        "gc_pct":        str(gc_pct),
        "gc_time":       _fmt_ms(t["jvmGcTime"]),
        "elapsed":       _fmt_elapsed(elapsed_s),
        # Stage-specific info
        "max_shuffle_stage":   f"Stage {max_shuffle_stage_id}" if max_shuffle_stage_id is not None else "a stage",
        "max_shuffle_bytes":   _fmt_bytes(max_shuffle_bytes),
        "max_spill_stage":     f"Stage {max_spill_stage_id}" if max_spill_stage_id is not None else "a stage",
        "max_spill_bytes":     _fmt_bytes(max_spill_bytes),
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
            "Wall-clock time from cell start to finish",
            _fmt_elapsed(elapsed_s)),
        _metric_row(
            "Parallel tasks used",
            "How many parallel pieces Spark split the work into. "
            "Healthy = 50 to 2000. Low on large data = cluster underused.",
            str(t["numTasks"]),
            highlight=few_tasks),
        _metric_row(
            "Data read from S3 / HDFS",
            "Total bytes Spark read from storage in this cell",
            _fmt_bytes(t["inputBytes"])),
        _metric_row(
            "Data written to S3 / HDFS",
            "Total bytes Spark wrote to storage in this cell",
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

    # ML library detection advice
    if ml_libs:
        ml_names = ", ".join(ml_libs)
        ml_card = _advice_card(
            "warn",
            "ML library detected — consider caching",
            f"Your cell uses {ml_names} with a Spark DataFrame.",
            "If you reuse the same DataFrame across multiple cells or operations, "
            "you should cache it to avoid re-reading from storage. "
            "ML operations often trigger multiple passes over the same data.",
            "Before passing the DataFrame to your ML library, cache it.",
            "# Cache the DataFrame BEFORE ML operations\n"
            "df = spark.read.parquet('s3://...')\n"
            "df = df.filter(df.year == 2024)  # Apply filters first\n"
            "df = df.cache()                   # Cache BEFORE ML\n"
            "df.count()                        # Force cache load\n\n"
            "# Now use with ML library\n"
            "ml_model.fit(df, y)"
        )
        cards_html = ml_card + cards_html

    if not fired:
        cards_html = f"""
        <div style="margin-top:12px;border-left:4px solid {_THEME['ok']['bar']};
                    border-radius:0 8px 8px 0;background:{_THEME['ok']['bg']};
                    padding:12px 16px;font-size:12px;color:#1B5E20;
                    font-family:Arial,sans-serif;line-height:1.6">
          <strong>No issues detected.</strong> Your cell ran efficiently.<br>
          If the run time still feels slow, the data volume itself may be the
          limiting factor rather than how Spark is configured.
        </div>"""

    # Full report
    display(HTML(f"""
    <div style="font-family:Arial,sans-serif;margin:10px 0;max-width:660px">

      <div style="background:#1C1917;color:#FDE68A;padding:8px 14px;
                  border-radius:8px 8px 0 0;font-size:13px;font-weight:700;
                  display:flex;justify-content:space-between;align-items:center">
        <span>⚡ sparkmonitor</span>
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

      <details style="border:1px solid #ddd;border-top:none">
        <summary style="padding:7px 14px;font-size:12px;font-weight:600;
                        color:#555;cursor:pointer;background:#F9F9F7;
                        user-select:none">
          ▸ Per-stage breakdown (click to expand)
        </summary>
        {_per_stage_breakdown(stages)}
      </details>

      {cards_html}

      <div style="margin-top:8px;font-size:10px;color:#aaa;text-align:right">
        sparkmonitor · reads Spark REST API · no external dependencies
      </div>

    </div>"""))


# ═══════════════════════════════════════════════════════════════
#  SECTION 2: CELL MAGIC (%%measure command)
#  ═════════════════════════════════════════════════════════════
#  This is what runs when you add %%measure to a cell.
#  It measures cell execution, captures Spark metrics, and shows a report.
# ═══════════════════════════════════════════════════════════════

# GLOBAL STATE: Connection info determined when module is loaded
_HOST, _PORT = _find_spark_port_and_host()
_APP_ID = _get_app_id(_HOST, _PORT) if _HOST and _PORT else None


def _get_jupyter_namespace():
    """Get the Jupyter kernel's user namespace for exec().

    This allows exec() to access user-defined variables like spark, df, etc.
    """
    try:
        from IPython import get_ipython
        ipython = get_ipython()
        if ipython:
            return ipython.user_ns
    except Exception:
        pass

    # Fallback to module globals if not in Jupyter
    return globals()


def _detect_ml_patterns(cell_code):
    """Detect ML library usage that should trigger caching recommendations."""
    import re

    patterns = {
        "sklearn": r"(?:from\s+sklearn|import\s+sklearn|RandomForest|SVM|LogisticRegression|KMeans)",
        "xgboost": r"(?:import\s+xgboost|xgb\.train|XGBClassifier|XGBRegressor)",
        "lightgbm": r"(?:import\s+lightgbm|lgb\.train|LGBMClassifier|LGBMRegressor)",
        "tensorflow": r"(?:import\s+tensorflow|model\.fit)",
        "pytorch": r"(?:import\s+torch|optimizer\.step)",
        "fit_check": r"(?:\.fit\()",
    }

    detected = []
    for lib, pattern in patterns.items():
        if re.search(pattern, cell_code, re.IGNORECASE):
            detected.append(lib)

    return detected


@register_cell_magic
def measure(line, cell):
    """Spark performance monitor cell magic.

    Usage:
        %%measure
        df = spark.read.parquet("s3://bucket/data/")
        df.groupBy("country").count().show()

    What happens:
    1. Executes your code
    2. Captures which Spark stages completed
    3. Measures elapsed time
    4. Waits for Spark History Server to index metrics
    5. Analyzes metrics and generates a performance report
    """

    # STEP 1: Get Jupyter namespace so exec() can access user variables
    # (This is needed because exec() runs in the module's scope, not the user's)
    user_ns = _get_jupyter_namespace()

    # STEP 2: Check if Spark connection was established during module load
    if not _HOST or not _PORT or not _APP_ID:
        display(HTML("""
        <div style="font-family:Arial,sans-serif;max-width:560px;padding:12px 16px;
                    background:#FFF0F0;border-left:4px solid #C0392B;
                    border-radius:0 8px 8px 0;font-size:13px;color:#7B1A1A">
          <strong>sparkmonitor: could not connect to Spark.</strong><br>
          <span style="font-size:12px;line-height:1.7">
          Make sure you have an active SparkSession before running %%measure.<br>
          If you just started the kernel, re-run Cell 1 after the SparkSession is ready.<br><br>
          On EMR/YARN clusters, sparkmonitor will try to auto-detect the Spark UI
          from sparkContext. If detection still fails, check that the SparkSession is initialized.
          </span>
        </div>"""))
        # Still execute the cell code so we don't break their workflow
        exec(cell, user_ns)
        return

    # STEP 3: Record which stages are already complete
    # (We only care about NEW stages that appear after this cell runs)
    before = _stage_ids_done(_HOST, _PORT, _APP_ID)

    # STEP 4: Time and execute user's code
    t0 = time.monotonic()
    exec(cell, user_ns)  # Execute in user's Jupyter namespace (so 'spark', 'df', etc. work)
    elapsed = time.monotonic() - t0

    # STEP 5: Wait for Spark History Server to index the new stages
    # History Server has latency (1-5 seconds), so we use smart adaptive waits
    # Hard timeout: Never wait more than 3 minutes (180 seconds) for stages to complete
    display(HTML("""
    <div style="font-family:Arial,sans-serif;max-width:600px;padding:8px 12px;
                background:#E8F4F8;border-left:4px solid #0277BD;
                border-radius:0 4px 4px 0;font-size:12px;color:#01579B;margin:8px 0">
      ⏳ <strong>Waiting for new stages to complete and be indexed by History Server...</strong>
      <span style="font-size:11px;color:#0277BD">(Timeout: 3 minutes maximum)</span>
    </div>"""))
    new = _capture_stages_with_retry(_HOST, _PORT, _APP_ID, before, elapsed)

    # Check if we hit the timeout
    if not new:
        display(HTML("""
        <div style="font-family:Arial,sans-serif;max-width:600px;padding:8px 12px;
                    background:#FFF3E0;border-left:4px solid #F57C00;
                    border-radius:0 4px 4px 0;font-size:12px;color:#E65100;margin:8px 0">
          ⚠️ <strong>Timeout waiting for new stages (3 minutes elapsed)</strong><br>
          <span style="font-size:11px">
          Your job may still be running. Possible causes:<br>
          • Job is still executing (see progress bar)<br>
          • Event logging not enabled<br>
          • History Server not accessible<br>
          See TROUBLESHOOTING.md for debugging steps.
          </span>
        </div>"""))

    # STEP 6: Detect if ML libraries are being used (for caching advice)
    ml_libs = _detect_ml_patterns(cell)

    # STEP 7: Render the performance report
    _render_report(new, elapsed, ml_libs)


# ═══════════════════════════════════════════════════════════════
#  STARTUP MESSAGE — shown when modeller runs Cell 1
# ═══════════════════════════════════════════════════════════════

if _HOST and _PORT and _APP_ID:
    display(HTML(f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:6px 0">
      <div style="background:#1C1917;color:#FDE68A;padding:9px 14px;
                  border-radius:8px 8px 0 0;font-size:13px;font-weight:700">
        ⚡ sparkmonitor is ready
      </div>
      <div style="border:1px solid #ccc;border-top:none;border-radius:0 0 8px 8px;
                  background:#FAFAF8;padding:14px 16px;font-size:12px;
                  color:#333;line-height:1.8">

        <strong>Connected</strong> to Spark at <code>{_HOST}:{_PORT}</code>
        (App ID: <code>{_APP_ID}</code>)<br>
        <span style="font-size:11px;color:#666">[History Server for live metrics, YARN RM for driver location, or direct Spark UI]</span><br><br>

        <strong>What to do next:</strong><br>
        Add <code style="background:#eee;padding:1px 5px;border-radius:3px">%%measure</code>
        as the very first line of any cell you want to profile.<br><br>

        <strong>Example:</strong>
        <pre style="background:#1C1917;color:#6EE7B7;padding:9px 12px;
                    border-radius:5px;font-size:11px;margin:6px 0 10px 0;
                    white-space:pre-wrap;line-height:1.6">%%measure
df = spark.read.parquet("s3://your-bucket/data/")
df.filter(df.year == 2024).groupBy("region").count().show()</pre>

        <strong>After the cell runs, you will see:</strong><br>
        &nbsp; 🟢 &nbsp;<strong>Green banner</strong> — no issues, cell ran efficiently<br>
        &nbsp; 🟡 &nbsp;<strong>Yellow warning</strong> — something is slow;
                  a plain-English explanation and copy-paste fix are shown<br>
        &nbsp; 🔴 &nbsp;<strong>Red alert</strong> — a serious issue that will slow
                  or crash the job; a fix is shown<br><br>

        You do not need to understand Spark internals.
        Every warning tells you exactly what happened, why it matters,
        and what code to change.

      </div>
    </div>"""))

elif _HOST and _PORT and not _APP_ID:
    display(HTML("""
    <div style="font-family:Arial,sans-serif;max-width:560px;margin:6px 0;
                padding:12px 16px;background:#FFF8E6;border-left:4px solid #E8A000;
                border-radius:0 8px 8px 0;font-size:13px;color:#7A4900">
      <strong>sparkmonitor loaded, but could not get Spark application ID.</strong><br>
      <span style="font-size:12px;line-height:1.7">
      Re-run this cell once your SparkSession is fully initialized,
      or place it after the cell that creates the SparkSession.
      </span>
    </div>"""))

else:
    display(HTML("""
    <div style="font-family:Arial,sans-serif;max-width:700px;margin:6px 0;
                padding:12px 16px;background:#FFF8E6;border-left:4px solid #E8A000;
                border-radius:0 8px 8px 0;font-size:13px;color:#7A4900">
      <strong>sparkmonitor loaded, but could not connect to Spark UI or History Server.</strong><br>
      <span style="font-size:12px;line-height:1.7">
      Make sure you have an active SparkSession initialized.<br><br>
      <strong>Quick checks:</strong><br>
      • Is Spark initialized? <code>print(spark.sparkContext.applicationId)</code><br>
      • Can you reach YARN RM? <code>curl http://localhost:8088/ws/v1/cluster/apps</code><br>
      • Can you reach History Server? <code>curl http://localhost:18080/api/v1/applications</code><br><br>
      <strong>If both are accessible:</strong><br>
      • Re-run this cell after the SparkSession is fully ready<br>
      • Spark may still be initializing—wait a moment and try again
      </span>
    </div>"""))
