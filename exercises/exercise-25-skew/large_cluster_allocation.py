# ═══════════════════════════════════════════════════════════════════════════
# EMR Resource Allocation Calculator
# Cluster: 1 Master + 2 Core + 60 Task Nodes
# Task Node Specs: 64 cores, 512 GB RAM
# ═══════════════════════════════════════════════════════════════════════════

# 🎯 CLUSTER OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════
"""
Total Task Nodes: 60
Per Task Node:
  - Total RAM: 512 GB
  - Total vCores: 64 cores
  - Usable RAM: ~460 GB (after OS/YARN overhead ~10%)
  - Usable Cores: 62-63 (leaving 1-2 for OS/YARN)
"""

# ═══════════════════════════════════════════════════════════════════════════
# EXECUTOR SIZING OPTIONS
# ═══════════════════════════════════════════════════════════════════════════

# IMPORTANT: For large nodes like yours (64 cores, 512GB), 
# it's BETTER to have multiple smaller executors per node rather than 
# one giant executor because:
# - Better parallelism
# - Lower GC overhead
# - Better fault tolerance
# - More stable performance

# ═══════════════════════════════════════════════════════════════════════════
# OPTION 1: 8 EXECUTORS PER NODE (RECOMMENDED for 64-core nodes)
# ═══════════════════════════════════════════════════════════════════════════
"""
Best for: Large-scale production workloads, balanced performance

CONFIGURATION:
--executor-memory 55g
--executor-cores 7
--conf spark.executor.memoryOverhead=12g
--conf spark.dynamicAllocation.maxExecutors=480

CALCULATION:
- Usable RAM per node: 460 GB
- RAM per executor: 55g + 12g overhead = 67 GB
- Executors per node: 460 / 67 = ~6.8 → Use 7-8 executors
- Cores per executor: 64 / 8 = 8 cores (use 7 for stability)
- Max executors: 60 nodes × 8 = 480 executors

PROS:
✅ Optimal parallelism
✅ Low GC pressure
✅ Good fault tolerance
✅ Balanced CPU/memory ratio

CONS:
❌ More executors = slightly more overhead
"""

OPTION_1_RECOMMENDED = {
    "executor_memory": "55g",
    "executor_cores": "7",
    "executor_memoryOverhead": "12g",
    "maxExecutors": "480",
    "executors_per_node": 8,
    "total_memory_per_executor": "67 GB"
}


# ═══════════════════════════════════════════════════════════════════════════
# OPTION 2: 4 EXECUTORS PER NODE (Balanced)
# ═══════════════════════════════════════════════════════════════════════════
"""
Best for: Memory-intensive workloads, large caching needs

CONFIGURATION:
--executor-memory 100g
--executor-cores 15
--conf spark.executor.memoryOverhead=20g
--conf spark.dynamicAllocation.maxExecutors=240

CALCULATION:
- RAM per executor: 100g + 20g overhead = 120 GB
- Executors per node: 460 / 120 = ~3.8 → Use 4 executors
- Cores per executor: 64 / 4 = 16 cores (use 15 for stability)
- Max executors: 60 nodes × 4 = 240 executors

PROS:
✅ More memory per executor
✅ Good for caching large datasets
✅ Fewer executors = less overhead

CONS:
❌ Higher GC overhead
❌ Less parallelism
"""

OPTION_2_BALANCED = {
    "executor_memory": "100g",
    "executor_cores": "15",
    "executor_memoryOverhead": "20g",
    "maxExecutors": "240",
    "executors_per_node": 4,
    "total_memory_per_executor": "120 GB"
}


# ═══════════════════════════════════════════════════════════════════════════
# OPTION 3: 2 EXECUTORS PER NODE (Memory-Heavy)
# ═══════════════════════════════════════════════════════════════════════════
"""
Best for: Extremely memory-intensive workloads, large ML models

CONFIGURATION:
--executor-memory 200g
--executor-cores 30
--conf spark.executor.memoryOverhead=30g
--conf spark.dynamicAllocation.maxExecutors=120

CALCULATION:
- RAM per executor: 200g + 30g overhead = 230 GB
- Executors per node: 460 / 230 = 2 executors
- Cores per executor: 64 / 2 = 32 cores (use 30 for stability)
- Max executors: 60 nodes × 2 = 120 executors

PROS:
✅ Maximum memory per executor
✅ Great for huge caching needs
✅ Good for ML/large model workloads

CONS:
❌ Very high GC overhead
❌ Limited parallelism
❌ Risk of GC pauses
"""

OPTION_3_MEMORY_HEAVY = {
    "executor_memory": "200g",
    "executor_cores": "30",
    "executor_memoryOverhead": "30g",
    "maxExecutors": "120",
    "executors_per_node": 2,
    "total_memory_per_executor": "230 GB"
}


# ═══════════════════════════════════════════════════════════════════════════
# OPTION 4: 1 EXECUTOR PER NODE (Not Recommended!)
# ═══════════════════════════════════════════════════════════════════════════
"""
Best for: Almost never! Avoid this configuration.

CONFIGURATION:
--executor-memory 400g
--executor-cores 60
--conf spark.executor.memoryOverhead=50g
--conf spark.dynamicAllocation.maxExecutors=60

CALCULATION:
- RAM per executor: 400g + 50g overhead = 450 GB
- Executors per node: 1
- Cores per executor: 60-62 cores
- Max executors: 60 nodes × 1 = 60 executors

PROS:
✅ Maximum resources per executor

CONS:
❌ Terrible GC performance (will spend most time in GC!)
❌ Very limited parallelism
❌ High memory overhead
❌ Single point of failure per node
❌ NOT RECOMMENDED!
"""

OPTION_4_NOT_RECOMMENDED = {
    "executor_memory": "400g",
    "executor_cores": "60",
    "executor_memoryOverhead": "50g",
    "maxExecutors": "60",
    "executors_per_node": 1,
    "total_memory_per_executor": "450 GB",
    "warning": "⚠️ NOT RECOMMENDED - GC overhead will kill performance!"
}


# ═══════════════════════════════════════════════════════════════════════════
# RECOMMENDED CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                    RECOMMENDED SPARK CONFIGURATION                        ║
║                  For 64-core, 512GB Task Nodes                           ║
╚═══════════════════════════════════════════════════════════════════════════╝

🎯 BEST PRACTICE: 8 Executors per Node
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SPARK-SUBMIT COMMAND:
────────────────────────────────────────────────────────────────────────────
spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --executor-memory 55g \\
  --executor-cores 7 \\
  --conf spark.executor.memoryOverhead=12g \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.minExecutors=8 \\
  --conf spark.dynamicAllocation.maxExecutors=480 \\
  --conf spark.dynamicAllocation.initialExecutors=16 \\
  your_script.py

OR IN JUPYTER NOTEBOOK:
────────────────────────────────────────────────────────────────────────────
spark = SparkSession.builder \\
    .appName("YourJob") \\
    .config("spark.executor.memory", "55g") \\
    .config("spark.executor.cores", "7") \\
    .config("spark.executor.memoryOverhead", "12g") \\
    .config("spark.dynamicAllocation.enabled", "true") \\
    .config("spark.dynamicAllocation.minExecutors", "8") \\
    .config("spark.dynamicAllocation.maxExecutors", "480") \\
    .config("spark.dynamicAllocation.initialExecutors", "16") \\
    .getOrCreate()

CLUSTER CAPACITY:
────────────────────────────────────────────────────────────────────────────
Total Available Executors:     480 (60 nodes × 8 executors)
Memory per Executor:           67 GB (55g + 12g overhead)
Cores per Executor:            7 cores
Total Cluster Memory:          ~40 TB
Total Cluster Cores:           3,840 cores

PER USER ALLOCATION (5 concurrent users):
────────────────────────────────────────────────────────────────────────────
Max Executors per User:        96 (20% of cluster)
For Large Jobs:                192 (40% of cluster)
For Exploratory:               48 (10% of cluster)
""")


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION FOR DIFFERENT JOB TYPES
# ═══════════════════════════════════════════════════════════════════════════

# Small Exploratory Jobs (10% of cluster)
EXPLORATORY_CONFIG = {
    "executor_memory": "55g",
    "executor_cores": "7",
    "executor_memoryOverhead": "12g",
    "maxExecutors": "48",  # 10% of 480
    "description": "For quick analysis and testing"
}

# Medium ETL Jobs (20% of cluster)
MEDIUM_CONFIG = {
    "executor_memory": "55g",
    "executor_cores": "7",
    "executor_memoryOverhead": "12g",
    "maxExecutors": "96",  # 20% of 480
    "description": "For typical daily ETL jobs"
}

# Large Production Jobs (40% of cluster)
LARGE_CONFIG = {
    "executor_memory": "55g",
    "executor_cores": "7",
    "executor_memoryOverhead": "12g",
    "maxExecutors": "192",  # 40% of 480
    "description": "For large production workloads"
}

# Full Cluster (during off-hours or dedicated jobs)
FULL_CLUSTER_CONFIG = {
    "executor_memory": "55g",
    "executor_cores": "7",
    "executor_memoryOverhead": "12g",
    "maxExecutors": "480",  # 100% of cluster
    "description": "For massive batch jobs during off-hours"
}


# ═══════════════════════════════════════════════════════════════════════════
# MEMORY-INTENSIVE ALTERNATIVE
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║              ALTERNATIVE: For Memory-Intensive Workloads                  ║
║                    (ML, Large Caching, Analytics)                        ║
╚═══════════════════════════════════════════════════════════════════════════╝

🎯 4 Executors per Node
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CONFIGURATION:
────────────────────────────────────────────────────────────────────────────
--executor-memory 100g
--executor-cores 15
--conf spark.executor.memoryOverhead=20g
--conf spark.dynamicAllocation.maxExecutors=240

CLUSTER CAPACITY:
────────────────────────────────────────────────────────────────────────────
Total Available Executors:     240 (60 nodes × 4 executors)
Memory per Executor:           120 GB (100g + 20g overhead)
Cores per Executor:            15 cores
""")


# ═══════════════════════════════════════════════════════════════════════════
# ABSOLUTE MAXIMUM VALUES (For Reference Only)
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                        ABSOLUTE MAXIMUM VALUES                            ║
║                    (Theoretical - Not Recommended!)                       ║
╚═══════════════════════════════════════════════════════════════════════════╝

⚠️ These are THEORETICAL maximums. DO NOT USE these values!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Per Task Node (512 GB RAM, 64 cores):
────────────────────────────────────────────────────────────────────────────
Max --executor-memory:         450g (leaving 62g for overhead/OS)
Max --executor-cores:          62 (leaving 2 for OS/YARN)
Max executors per node:        1 (if using above values)

Cluster-Wide:
────────────────────────────────────────────────────────────────────────────
Absolute Max Executors:        60 (1 per node with max settings)

⚠️  WARNING: DO NOT USE THESE VALUES!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Using maximum memory/cores per executor will result in:
  ❌ Extremely poor GC performance (90%+ time in GC)
  ❌ Limited parallelism
  ❌ Memory fragmentation
  ❌ Job failures
  ❌ Unstable performance

Instead, use the RECOMMENDED configuration with 8 executors per node!
""")


# ═══════════════════════════════════════════════════════════════════════════
# PARTITION SIZING FOR YOUR CLUSTER
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                      PARTITION RECOMMENDATIONS                            ║
╚═══════════════════════════════════════════════════════════════════════════╝

With 480 total executors (7 cores each):
────────────────────────────────────────────────────────────────────────────
Total Available Cores:         3,360 cores (480 executors × 7 cores)

Optimal Parallelism:
- spark.default.parallelism:   3360 (or 2-3× this = 6720-10080)
- spark.sql.shuffle.partitions: Depends on data size
  
For 128MB partitions:
  Data Size    →  Partitions
  ──────────────────────────
  100 GB       →  800
  1 TB         →  8,000
  10 TB        →  80,000
  
Configure:
────────────────────────────────────────────────────────────────────────────
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
spark.conf.set("spark.default.parallelism", "6720")  # 2× cores
spark.conf.set("spark.sql.shuffle.partitions", "<calculated>")  # Based on data size
""")


# ═══════════════════════════════════════════════════════════════════════════
# COMPLETE RECOMMENDED CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

COMPLETE_CONFIG = """
# Complete Spark Configuration for 64-core, 512GB Task Nodes
# =============================================================

spark = SparkSession.builder \\
    .appName("Production_Job") \\
    \\
    # Executor Configuration (8 executors per node)
    .config("spark.executor.memory", "55g") \\
    .config("spark.executor.cores", "7") \\
    .config("spark.executor.memoryOverhead", "12g") \\
    \\
    # Dynamic Allocation
    .config("spark.dynamicAllocation.enabled", "true") \\
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \\
    .config("spark.dynamicAllocation.minExecutors", "8") \\
    .config("spark.dynamicAllocation.maxExecutors", "96") \\  # Adjust per user
    .config("spark.dynamicAllocation.initialExecutors", "16") \\
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \\
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "300s") \\
    \\
    # Driver Configuration
    .config("spark.driver.memory", "16g") \\
    .config("spark.driver.maxResultSize", "4g") \\
    .config("spark.driver.memoryOverhead", "4g") \\
    \\
    # Partition Configuration
    .config("spark.sql.files.maxPartitionBytes", "134217728") \\  # 128 MB
    .config("spark.default.parallelism", "6720") \\  # 2× total cores
    .config("spark.sql.shuffle.partitions", "800") \\  # Adjust based on data
    \\
    # Adaptive Query Execution
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") \\
    \\
    # Serialization
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .config("spark.kryoserializer.buffer.max", "512m") \\
    \\
    # Performance Tuning
    .config("spark.speculation", "true") \\
    .config("spark.speculation.multiplier", "2") \\
    .config("spark.network.timeout", "800s") \\
    .config("spark.executor.heartbeatInterval", "60s") \\
    \\
    .getOrCreate()
"""

print(COMPLETE_CONFIG)


# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY TABLE
# ═══════════════════════════════════════════════════════════════════════════

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                         QUICK REFERENCE TABLE                             ║
╚═══════════════════════════════════════════════════════════════════════════╝

Configuration      Exec/Node  Exec-Mem  Exec-Cores  Max-Exec  Use Case
─────────────────────────────────────────────────────────────────────────────
RECOMMENDED        8          55g       7           480       General workloads
Memory-Intensive   4          100g      15          240       ML, caching
Light workloads    8          55g       7           96        Per-user limit
Exploratory        8          55g       7           48        Testing
─────────────────────────────────────────────────────────────────────────────

ANSWERS TO YOUR QUESTIONS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Q: Max --executor-memory?
A: 55g (Recommended) or 100g (Memory-intensive)
   
Q: Max --executor-cores?
A: 7 (Recommended) or 15 (Memory-intensive)
   
Q: Max spark.dynamicAllocation.maxExecutors?
A: 480 (Full cluster) or 96 (Per user with 5 concurrent users)

RECOMMENDATION: Use 8 executors per node configuration for best performance!
""")
