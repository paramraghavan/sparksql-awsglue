# 🚀 EMR Jupyter Quick Reference Card
**For Shared Cluster: 1 Master + 2 Core + 100 Task Nodes**

---

## ⚡ Essential Spark Config (Copy & Paste This!)

```python
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName(f"YourName_Job_{datetime.now().strftime('%Y%m%d_%H%M')}") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.dynamicAllocation.initialExecutors", "2") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

---

## 📊 Resource Guidelines by Job Size

| Job Type | Data Size | Max Executors | Executor Memory | Executor Cores |
|----------|-----------|---------------|-----------------|----------------|
| **Small** | < 10 GB | 10 | 4g | 2 |
| **Medium** | 10-100 GB | 20 | 8g | 4 |
| **Large** | > 100 GB | 40 | 12g | 4 |

**Calculation**: With 100 task nodes, if 5 users are active → 20 executors/user is fair

---

## ✅ DO's

1. ✅ **Always use Dynamic Allocation** (already in config above)
2. ✅ **Name your jobs**: Include your name and date
3. ✅ **Stop Spark when done**: `spark.stop()`
4. ✅ **Unpersist cached data**: `df.unpersist()`
5. ✅ **Monitor Spark UI**: http://localhost:4040

---

## ❌ DON'Ts

1. ❌ **Never use static allocation**: `.config("spark.executor.instances", "50")`
2. ❌ **Don't hog resources**: Limit maxExecutors based on job needs
3. ❌ **Don't leave idle sessions**: Stop when not actively using
4. ❌ **Don't cache everything**: Only cache DataFrames you reuse 3+ times
5. ❌ **Don't ignore the cleanup**: Always run cleanup cell!

---

## 🔍 Health Check (Run Before Starting)

```python
import requests

def check_cluster():
    try:
        r = requests.get("http://localhost:8088/cluster/cluster", timeout=10)
        if r.status_code == 200:
            print("✅ Cluster is healthy")
            return True
        print(f"⚠️ Issue detected: Status {r.status_code}")
        return False
    except:
        print("❌ Cannot reach cluster!")
        raise

check_cluster()
```

---

## 🧹 Cleanup (Always Run When Done!)

```python
# Clear cache
spark.catalog.clearCache()

# Stop session
spark.stop()

print("✅ Resources released back to cluster!")
```

---

## 🐛 Common Issues & Quick Fixes

### "Job is slow"
- Check partitions: `df.rdd.getNumPartitions()`
- Repartition: `df = df.repartition(200)`
- Use Parquet instead of CSV

### "Out of Memory"
- Reduce executor cores to 2-3
- Increase memoryOverhead to 3g
- Process data in smaller chunks

### "Container killed by YARN"
- Lower maxExecutors to 10-15
- Reduce executor.memory to 6g
- Check if cluster is busy: http://localhost:8088

### "Cannot allocate executors"
- Wait 2-3 minutes for resources
- Check if others have idle sessions
- Try during off-peak hours

---

## 📱 Monitoring URLs

| Service | URL | Purpose |
|---------|-----|---------|
| **Spark UI** | http://localhost:4040 | Your job details |
| **YARN** | http://localhost:8088 | Cluster resources |
| **History Server** | http://localhost:18080 | Past jobs |

---

## 💡 Pro Tips

1. **Filter First**: Always filter data before joins/aggregations
2. **Broadcast Small Tables**: Use `.broadcast()` for tables < 10MB
3. **Partition Wisely**: Aim for 128MB per partition
4. **Use Parquet**: 5-10x faster than CSV
5. **Explain Plans**: Run `df.explain()` to understand query execution

---

## 📐 Executor Sizing Formula

**Task Node**: ~32GB RAM, 8 vCores

**Recommended per Executor**:
- Memory: 8GB (leaves room for OS and overhead)
- Cores: 4 (optimal for I/O parallelism)
- **Result**: ~3-4 executors per task node

**For 100 task nodes**: 300-400 total potential executors

**Fair share per user** (5 active users): 20 executors

---

## 🚨 Critical Rules

### Rule #1: Dynamic Allocation is MANDATORY
Never set fixed executor counts. Always use:
```python
.config("spark.dynamicAllocation.enabled", "true")
```

### Rule #2: Set Max Executors Appropriately
- Exploratory work: 10 max
- Standard ETL: 20 max  
- Large batch jobs: 40 max
- Never use > 50 on shared cluster

### Rule #3: Always Cleanup
Resources aren't released until you call `spark.stop()`

---

## 📞 Getting Help

**Slow Job?** Check Spark UI → Stages → Look for data skew

**Out of Resources?** Check YARN UI → See who's using what

**Cluster Down?** Email: cluster-admin@company.com

**General Questions?** Slack: #data-engineering

---

## 🎯 Perfect Notebook Structure

1. **Cell 1**: Health check
2. **Cell 2**: Create Spark session (with config above)
3. **Cell 3**: Your data processing
4. **Cell 4**: Results/output
5. **Last Cell**: Cleanup (unpersist + spark.stop())

---

**Remember**: We all share this cluster! Being a good cluster citizen helps everyone work efficiently. 🤝

**Last Updated**: January 2026 | Questions? → data-team@company.com
