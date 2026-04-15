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
        initial_wait = 3.0  # Increased from 1.5
        max_retries = 8  # Increased from 5
        retry_interval = 1.5  # Increased from 1.0
    elif elapsed < 2.0:
        # Quick job
        initial_wait = 2.0  # Increased from 1.0
        max_retries = 6  # Increased from 4
        retry_interval = 1.0  # Increased from 0.8
    elif elapsed < 5.0:
        # Normal job
        initial_wait = 1.0  # Increased from 0.6
        max_retries = 5  # Increased from 3
        retry_interval = 0.8  # Increased from 0.6
    else:
        # Long job - lag is negligible
        initial_wait = 0.8  # Increased from 0.6
        max_retries = 2  # Increased from 1
        retry_interval = 0.6  # Same

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
    if ms < 60_000: return f"{ms / 1000:.1f} s"
    return f"{ms / 60_000:.1f} min"


def _fmt_elapsed(s):
    if s < 60:  return f"{s:.1f} s"
    return f"{int(s // 60)} min {int(s % 60)} s"


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

    # ─── 1. DISK SPILL ──────────────────────────────────────────────────────────
    # Any disk spill is an error: executor ran out of RAM.
    dict(
        trigger=lambda t, e: t["diskBytesSpilled"] > 0,
        level="error",
        title="Data spill to disk: potential memory exhaustion",
        what="Spark ran out of RAM in {max_spill_stage} and spilled "
             "{max_spill_bytes} to the cluster's local NVMe/SSD storage.",
        why="Reading from local disk is significantly slower than RAM. This is usually caused by "
            "data skew (one partition is too big) or having too few partitions for the data volume.",
        fix="1. Increase the number of partitions using .repartition(N).\n"
            "2. Use .cache() on DataFrames that are reused across multiple operations.\n"
            "3. Check for data skew — if one key has millions of rows while others have hundreds, "
            "one executor drowns while the rest sit idle.",
        code="# Increase parallelism to shrink individual partition size:\n"
             "df = df.repartition(1000)\n\n"
             "# Or cache if the DataFrame is reused:\n"
             "df.cache().count()  # .count() forces the cache to load eagerly",
    ),

    # ─── 2. DATA SKEW (proxy) ───────────────────────────────────────────────────
    dict(
        # Trigger logic:
        # 1. Significant data was moved (200MB+)
        # 2. At least one task spilled to disk
        # 3. The 'max' shuffle/spill is > 3x the 'median' (The definition of skew)
        trigger=lambda t, e: (
                t["shuffleWriteBytes"] > 200 * 1024 ** 2
                and t["diskBytesSpilled"] > 0
                and t.get("maxTaskShuffleWrite", 0) > (t.get("medianTaskShuffleWrite", 0) * 3)
        ),
        level="warn",
        title="Data Skew Detected: Uneven workload distribution",
        what="One task is doing significantly more work than others. "
             "While most tasks finished quickly, one executor had to process "
             "{max_task_shuffle} of data and spilled to disk.",
        why="This happens when your join/groupBy key is not evenly distributed (e.g., millions of "
            "rows have a NULL key or a default '0' ID). One executor gets all that data, "
            "runs out of RAM, and the whole job waits for that one task to finish.",
        fix="1. **Check for NULLs**: Filters nulls before joining if they aren't needed.\n"
            "2. **AQE Skew Join**: Ensure Spark 3's automatic skew handling is on.\n"
            "3. **Salting**: If a real key (e.g., 'Country=US') is just massive, use 'salting' "
            "to break that one key into multiple parallel tasks.",
        code="# 1. Identify the 'heavy' keys:\n"
             "df.groupBy('join_key').count().sort('count', ascending=False).show()\n\n"
             "# 2. Enable Spark's Auto-Skew Fix:\n"
             "spark.conf.set('spark.sql.adaptive.enabled', 'true')\n"
             "spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')\n\n"
             "# 3. Salting example (to spread one key across 10 tasks):\n"
             "from pyspark.sql.functions import expr\n"
             "df = df.withColumn('salt', (rand() * 10).cast('int'))\n"
             "df = df.withColumn('salted_key', concat(col('join_key'), lit('_'), col('salt')))"
    ),
    # ─── 3. MEMORY PRESSURE ─────────────────────────────────────────────────────
    # memoryBytesSpilled > 0 but no disk spill yet: executor is near its RAM limit.
    # This is the "early warning" before the disk-spill error fires on a larger run.
    dict(
        trigger=lambda t, e: (
                t["memoryBytesSpilled"] > 0
                and t["diskBytesSpilled"] == 0  # disk-spill rule already covers the worse case
        ),
        level="warn",
        title="Memory pressure: RAM running low in executors",
        what="Spark spilled {mem_spill} between memory pools (execution ↔ storage) "
             "while staying off disk — but disk spill will follow if data volume grows.",
        why="Memory pressure means executors are near their RAM ceiling. The next run with "
            "slightly more data will tip into disk spill, which is significantly slower and "
            "can crash long jobs.",
        fix="1. Drop unused columns early — df.select('col1', 'col2') before any join or "
            "aggregation reduces the memory footprint of every subsequent stage.\n"
            "2. Repartition to smaller partition sizes so each task holds less data at once.\n"
            "3. If this is a recurring job, increase spark.executor.memory in the cluster config.",
        code="# Drop unused columns first to shrink the data each task must hold:\n"
             "df = df.select('only', 'needed', 'columns')\n\n"
             "# More partitions = less RAM per task:\n"
             "df = df.repartition(500)\n\n"
             "# Or increase executor memory (set before the SparkSession is created):\n"
             "spark.conf.set('spark.executor.memory', '8g')",
    ),

    # ─── 4. MODERATE SHUFFLE (warn) ─────────────────────────────────────────────
    # 500 MB – 1 GB: notable network cost but not yet critical.
    dict(
        trigger=lambda t, e: 500 * 1024 ** 2 < t["shuffleWriteBytes"] <= 1 * 1024 ** 3,
        level="warn",
        title="Moderate shuffle in {max_shuffle_stage} — room to optimise",
        what="Spark moved {shuffle_write} across the network. "
             "This adds latency but is not yet the dominant cost.",
        why="Every byte moved over the network costs time and cluster I/O. At this scale it is "
            "worth reducing now — your job will be more resilient as data grows.",
        fix="1. Drop unused columns before the join to reduce the amount shuffled.\n"
            "2. Raise the auto-broadcast threshold if the smaller table is around 50 MB.\n"
            "3. Use broadcast() explicitly if you know one side is small.",
        code="# Raise auto-broadcast threshold from 10 MB to 50 MB:\n"
             "spark.conf.set('spark.sql.autoBroadcastJoinThreshold', 50 * 1024 * 1024)\n\n"
             "# Select only the columns you need before joining:\n"
             "df_small = df_small.select('join_key', 'needed_col')\n\n"
             "# Or broadcast explicitly:\n"
             "from pyspark.sql.functions import broadcast\n"
             "result = df_big.join(broadcast(df_small), 'join_key')",
    ),

    # ─── 5. CRITICAL SHUFFLE (error) ────────────────────────────────────────────
    # > 1 GB: this all-to-all transfer is almost certainly the bottleneck.
    dict(
        trigger=lambda t, e: t["shuffleWriteBytes"] > 1 * 1024 ** 3,
        level="error",
        title="Critical shuffle in {max_shuffle_stage} — likely your main bottleneck",
        what="Spark moved {shuffle_write} across the network in {max_shuffle_stage}. "
             "This all-to-all transfer is likely why the job is slow.",
        why="Large shuffles force every executor to wait for network I/O before it can continue. "
            "At this scale you must change the join strategy or enable adaptive features — "
            "tuning partition counts alone will not be enough.",
        fix="1. Enable AQE — lets Spark coalesce small partitions and handle skew automatically.\n"
            "2. Broadcast join — best when one table fits in memory (under ~100 MB).\n"
            "3. Pre-shuffle on the join key — repartition both sides by the join key "
            "before the join so Spark does not have to do a cross-cluster exchange.",
        code="# Enable Adaptive Query Execution (on by default in EMR 6+):\n"
             "spark.conf.set('spark.sql.adaptive.enabled', 'true')\n\n"
             "# Option A — Broadcast join (best when one table is small):\n"
             "from pyspark.sql.functions import broadcast\n"
             "df = big_df.join(broadcast(small_df), 'id')\n\n"
             "# Option B — Pre-shuffle on join key (avoids cross-cluster exchange):\n"
             "df1 = df1.repartition(500, 'join_key')\n"
             "df2 = df2.repartition(500, 'join_key')\n"
             "df  = df1.join(df2, 'join_key')",
    ),

    # ─── 6. UNDER-PARALLELISED ──────────────────────────────────────────────────
    # < 10 tasks on > 100 MB of input: most CPUs are idle.
    dict(
        trigger=lambda t, e: t["numTasks"] < 10 and t["inputBytes"] > 100 * 1024 ** 2,
        level="warn",
        title="Under-parallelised: small-file or single-partition problem",
        what="Only {num_tasks} tasks processed {input_bytes} of data.",
        why="Spark creates one task per file or partition. A handful of large files means "
            "most of your cluster's CPUs sit idle. You are paying for a cluster but running "
            "like a single machine.",
        fix="Repartition the DataFrame to distribute work across all available executors. "
            "A good target is 2–3× the total CPU core count of your cluster.",
        code="# Repartition to use all available cores:\n"
             "df = df.repartition(200)\n\n"
             "# Alternatively, set the default shuffle partition count globally:\n"
             "spark.conf.set('spark.sql.shuffle.partitions', '400')",
    ),

    # ─── 7. OVER-PARTITIONED ────────────────────────────────────────────────────
    # > 1 000 tasks AND < 1 MB per task on average: task scheduling overhead
    # dominates useful work. The opposite problem to under-parallelisation.
    dict(
        trigger=lambda t, e: (
                t["numTasks"] > 1000
                and t["inputBytes"] > 0
                and (t["inputBytes"] / t["numTasks"]) < 1 * 1024 ** 2  # < 1 MB per task
        ),
        level="warn",
        title="Over-partitioned: too many tiny tasks",
        what="{num_tasks} tasks ran on {input_bytes} total — "
             "each task processed less than 1 MB of data on average.",
        why="Spark has fixed overhead for every task: scheduling, serialisation, and JVM "
            "bookkeeping. When partitions are tiny, this overhead dominates the actual "
            "computation. The cluster spends more time managing tasks than running them.",
        fix="Use coalesce() to reduce the partition count without a full reshuffle. "
            "A healthy target is 128 MB–512 MB of data per partition.",
        code="# coalesce() reduces partitions without a full shuffle (cheaper than repartition):\n"
             "df = df.coalesce(100)\n\n"
             "# Or cap the default shuffle partition count before any aggregation/join:\n"
             "spark.conf.set('spark.sql.shuffle.partitions', '200')\n\n"
             "# AQE can also coalesce small partitions automatically (Spark 3+):\n"
             "spark.conf.set('spark.sql.adaptive.enabled', 'true')\n"
             "spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')",
    ),

    # ─── 8. HIGH GC OVERHEAD ────────────────────────────────────────────────────
    # JVM spent > 10 % of executor run time on garbage collection.
    dict(
        trigger=lambda t, e: (
                t["executorRunTime"] > 0
                and t["jvmGcTime"] / t["executorRunTime"] > 0.10
        ),
        level="warn",
        title="High GC overhead — standard Python UDFs suspected",
        what="The JVM spent {gc_pct}% of executor time on garbage collection "
             "({gc_time}) instead of running your code.",
        why="Standard Python UDFs move data row-by-row between the JVM and Python. "
            "The JVM must create and destroy a new Java object for every single row, "
            "triggering frequent and expensive memory cleanups (GC pauses). "
            "These pauses block all other work on the executor.",
        fix="1. Best — replace the UDF with a built-in Spark SQL function "
            "(when(), regexp_replace(), date_trunc(), etc.). These run entirely in the JVM.\n"
            "2. Better — if custom Python logic is unavoidable, use a Pandas UDF "
            "(also called a vectorised UDF). It transfers data in Arrow batches instead of "
            "row-by-row, cutting object churn by orders of magnitude.\n"
            "3. If no UDFs are in use, try repartitioning to reduce per-executor data volume "
            "or increasing executor memory.",
        code="# SLOW — standard UDF (one Java object per row):\n"
             "from pyspark.sql.functions import udf\n"
             "@udf('double')\n"
             "def slow_plus_one(x):\n"
             "    return x + 1\n\n"
             "# FAST — Pandas UDF (Arrow-batched, avoids per-row object creation):\n"
             "import pandas as pd\n"
             "from pyspark.sql.functions import pandas_udf\n"
             "@pandas_udf('double')\n"
             "def fast_plus_one(s: pd.Series) -> pd.Series:\n"
             "    return s + 1\n\n"
             "df = df.withColumn('v', fast_plus_one(df['col']))",
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
        esc = code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
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
    bg = "background:#FFF8E6;" if highlight else "background:#fff;"
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
        shuffle_issue = shuffle_write > 500 * 1024 ** 2
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
        "numTasks": sum(s.get("numTasks", 0) for s in stages),
        "executorRunTime": sum(s.get("executorRunTime", 0) for s in stages),
        "executorCpuTime": sum(s.get("executorCpuTime", 0) for s in stages),
        "jvmGcTime": sum(s.get("jvmGcTime", 0) for s in stages),
        "shuffleReadBytes": sum(s.get("shuffleReadBytes", 0) for s in stages),
        "shuffleWriteBytes": sum(s.get("shuffleWriteBytes", 0) for s in stages),
        "maxTaskShuffleWrite": max(s.get("shuffleWriteBytes", 0) for s in stages),
        "medianTaskShuffleWrite": median(s.get("shuffleWriteBytes", 0) for s in stages),
        "diskBytesSpilled": sum(s.get("diskBytesSpilled", 0) for s in stages),
        "memoryBytesSpilled": sum(s.get("memoryBytesSpilled", 0) for s in stages),
        "inputBytes": sum(s.get("inputBytes", 0) for s in stages),
        "outputBytes": sum(s.get("outputBytes", 0) for s in stages),
        "peakExecutionMemory": max((s.get("peakExecutionMemory", 0) for s in stages), default=0),
    }

    # Find problem stages for stage-specific advice
    problem_stages = _find_problem_stages(stages)

    gc_pct = int(100 * t["jvmGcTime"] / t["executorRunTime"]) \
        if t["executorRunTime"] > 0 else 0

    # Template substitutions for advice text
    max_shuffle_stage_id, max_shuffle_bytes = problem_stages["max_shuffle_stage"]
    max_spill_stage_id, max_spill_bytes = problem_stages["max_spill_stage"]

    subs = {
        "disk_spill": _fmt_bytes(t["diskBytesSpilled"]),
        "mem_spill": _fmt_bytes(t["memoryBytesSpilled"]),
        "shuffle_write": _fmt_bytes(t["shuffleWriteBytes"]),
        "num_tasks": str(t["numTasks"]),
        "input_bytes": _fmt_bytes(t["inputBytes"]),
        "gc_pct": str(gc_pct),
        "gc_time": _fmt_ms(t["jvmGcTime"]),
        "elapsed": _fmt_elapsed(elapsed_s),
        # Stage-specific info
        "max_shuffle_stage": f"Stage {max_shuffle_stage_id}" if max_shuffle_stage_id is not None else "a stage",
        "max_shuffle_bytes": _fmt_bytes(max_shuffle_bytes),
        "max_spill_stage": f"Stage {max_spill_stage_id}" if max_spill_stage_id is not None else "a stage",
        "max_spill_bytes": _fmt_bytes(max_spill_bytes),
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
    has_warn = any(r["level"] == "warn" for r in fired)
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
    # These flags determine which metrics get highlighted in yellow (warning)
    spill_disk = t["diskBytesSpilled"] > 0  # ⚠️ ANY disk spill = problem (memory pressure)
    spill_mem = t["memoryBytesSpilled"] > 0  # ⚠️ ANY memory pressure = warning (precedes disk spill)
    big_shuffle = t["shuffleWriteBytes"] > 500 * 1024 ** 2  # ⚠️ > 500 MB shuffle = expensive network operation
    few_tasks = t["numTasks"] < 10 and t[
        "inputBytes"] > 100 * 1024 ** 2  # ⚠️ < 10 tasks on large data = cluster underused
    high_gc = gc_pct > 10  # ⚠️ > 10% GC time = memory overhead (Python UDFs common cause)

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
            "Time the JVM spent garbage collecting (cleaning up unused objects). "
            "⚠️ WARNING if > 10% of run time. Common causes: Python UDFs, slow shuffles, or memory pressure. "
            "Fix: Use Scala UDFs, repartition data, or increase executor memory.",
            f"{_fmt_ms(t['jvmGcTime'])}  ({gc_pct}% of run time)",
            highlight=high_gc),
        _metric_row(
            "Peak executor memory",
            "The most memory any single executor used at one time. "
            "If this is close to your executor memory limit (default 4GB), you will get disk spill on next run. "
            "Indicator: If this is > 80% of executor memory, increase executor memory or reduce data per partition.",
            _fmt_bytes(t["peakExecutionMemory"])),
    ])

    # Advice cards
    cards_html = ""
    for rule in fired:
        ti = rule["title"].format(**subs)  # ← add this
        w = rule["what"].format(**subs)
        y = rule["why"].format(**subs)
        f = rule["fix"].format(**subs)
        cards_html += _advice_card(rule["level"], ti, w, y, f, rule.get("code"))

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
          {len(stages)} stage{'s' if len(stages) != 1 else ''}
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
