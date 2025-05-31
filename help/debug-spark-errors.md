[spark-config-params.md](../exercises/exercise11_compare-files/spark-config-params.md)
[spark-submit-steps.md](../exercises/exercise11_compare-files/spark-submit-steps.md)

# Question 1

```markdown
Jupyter notebook job fails with error on running --> yarn application - status Application application_22222xxxxxx0128
Application application_22222xxxxxx0128 failed 2 times due to Application master for appattempt22222xxxxxx_0128
timed out Failing the application
```

# Answer 1

# Step1

## Key Things to Look For

**1. Filter for ERROR and FATAL messages first:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | grep -i "error\|fatal\|exception"
```

**2. Look for Application Master specific issues:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | grep -i "application master\|am container"
```

**3. Check for resource allocation problems:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | grep -i "resource\|memory\|insufficient\|allocation"
```

## Specific Error Patterns to Search For

- `OutOfMemoryError` - Memory issues
- `Container killed` - Resource limits exceeded
- `Connection refused` - Network/connectivity issues
- `Insufficient resources` - Cluster capacity problems
- `Queue capacity exceeded` - Queue limits hit
- `Authentication failed` - Permission issues

## Most Useful Log Sections

**Start with the Application Master logs:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 -containerId <AM_container_id>
```

**Look at the end of logs (most recent errors):**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | tail -100
```

**Check the beginning (startup issues):**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | head -100
```

Try the grep commands above first - they'll show you the key error messages without all the noise.

# Step 2

## Check the Container Type

**1. Look at the container ID in the OOM error:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | grep -i "outofmemory\|oom" -A 5 -B 5
```

The container ID pattern tells you:

- `container_xxx_000001` = **Driver/Application Master**
- `container_xxx_000002` and higher = **Executors/Tasks**

## Specific Checks

**2. Check for Driver OOM:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | grep -i "driver.*outofmemory\|application master.*outofmemory"
```

**3. Check for Executor OOM:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | grep -i "executor.*outofmemory\|task.*outofmemory"
```

**4. Look for heap space specifics:**

```bash
yarn logs -applicationId application_*22222*xxxxxx_0128 | grep -i "java heap space\|gc overhead limit"
```

## Solutions Based on Location

**If it's Driver OOM:**

```python
spark.conf.set("spark.driver.memory", "4g")  # Increase from default 1g
spark.conf.set("spark.driver.maxResultSize", "2g")
```

**If it's Executor OOM:**

```python
spark.conf.set("spark.executor.memory", "4g")  # Increase executor memory
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  # Reduce partition size
```

**If it's during data collection:**

```python
# Avoid collecting large datasets to driver
df.collect()  # Replace with df.write.mode("overwrite").saveAsTable("table_name")
```

Run the first grep command and paste a few lines of the OOM error - that will tell us exactly which container/component
is failing.

> Problem identified as it's out of memeory error how do i know if its on master/driver or task node

# Question 2

```text
i run pyspark job from juoyter notebook and on spark-master node with spark-submit
in both scenarions i allocate driver.memory to 48g
the jupyter-notebook jobs fails with Outof memory, the spark-submit job runs to successful completion
 explain difference when run from jupyter notebook and on master node with spark submit
```

# Answer 2

```markdown
The key difference is **WHERE the driver runs** and how memory is actually allocated. Here's what's
happens:

## Driver Location Difference

**Jupyter Notebook (Client Mode):**

- Driver runs **locally on the Jupyter server/your machine**
- Your `spark.driver.memory=48g` setting may be **ignored** if the Jupyter kernel doesn't have enough RAM
- The actual JVM running your notebook is limited by the machine's available memory

**spark-submit on Master Node (Cluster Mode):**

- Driver runs **on the cluster** where resources are properly allocated
- YARN actually provisions a container with 48GB for the driver
- Full cluster resources are available

## Memory Allocation Reality Check

**In Jupyter, check actual memory:**

```python
import psutil
print(f"Available RAM: {psutil.virtual_memory().total / (1024**3):.1f} GB")
print(f"Available RAM: {psutil.virtual_memory().available / (1024**3):.1f} GB")

# Check if Spark actually got your requested memory
print(f"Driver memory setting: {spark.conf.get('spark.driver.memory')}")
```

**The problem:** Even if you set `driver.memory=48g`, if your Jupyter server only has 16GB RAM total, the driver can't
actually use 48GB.

## Solutions for Jupyter

**1. Deploy mode (if supported):**

```python
spark.conf.set("spark.submit.deployMode", "cluster")  # Driver runs on cluster
```

**2. Resource management:**

```python
# Process data in smaller chunks
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.driver.maxResultSize", "8g")  # Limit data collection to driver
```

**3. Avoid driver-heavy operations:**

```python
# Instead of collecting large results
# df.collect()  # Brings all data to driver

# Use actions that don't collect to driver
df.write.mode("overwrite").saveAsTable("result_table")
df.count()  # Returns only a number
```

The fundamental issue is that Jupyter runs in client mode where your local machine's memory limits override Spark's
driver memory setting. What's the RAM capacity of your Jupyter server?

```

## How run like saprk-submit comman dline with jupter ntoebook
### Use environment variables (more reliable in Jupyter)

```python
import os

# Set BEFORE importing pyspark
os.environ['SPARK_DRIVER_MEMORY'] = '48g'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 48g --driver-max-result-size 16g pyspark-shell'

# Then import and create session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

### Check Jupyter server limitations:

```python
import psutil

total_ram = psutil.virtual_memory().total / (1024 ** 3)
available_ram = psutil.virtual_memory().available / (1024 ** 3)
print(f"Total system RAM: {total_ram:.1f} GB")
print(f"Available RAM: {available_ram:.1f} GB")
```

# All steps together

Here's how to properly implement the explicit JVM options approach:

## Method 1: Environment Variables (Most Reliable)

```python
import os

# Set this BEFORE importing any pyspark modules
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--driver-memory 48g --driver-max-result-size 16g --conf spark.driver.extraJavaOptions="-Xmx48g -XX:+UseG1GC" pyspark-shell'

# Now import and create session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

## Method 2: SparkConf with JVM Options

```python
from pyspark import SparkConf
from pyspark.sql import SparkSession

# Stop any existing sessions first
try:
    spark.stop()
except:
    pass

conf = SparkConf()
conf.set("spark.driver.memory", "48g")
conf.set("spark.driver.maxResultSize", "16g")
conf.set("spark.driver.extraJavaOptions", "-Xmx48g -XX:+UseG1GC -XX:MaxGCPauseMillis=200")
conf.set("spark.app.name", "MyApp")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

## Method 3: Jupyter Magic (if using Livy/EMR notebooks)

```python
%%configure - f
{
    "driverMemory": "48g",
    "driverMaxResultSize": "16g",
    "conf": {
        "spark.driver.extraJavaOptions": "-Xmx48g -XX:+UseG1GC"
    }
}
```

## Important Notes:

**1. Order matters - set environment variables first:**

```python
# This cell should be run FIRST, before any pyspark imports
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--driver-memory 48g --driver-max-result-size 16g --conf spark.driver.extraJavaOptions="-Xmx48g" pyspark-shell'
```

**2. Verify it worked:**

```python
# Run this AFTER creating SparkSession
print("Environment:", os.environ.get('PYSPARK_SUBMIT_ARGS'))
print("Driver memory:", spark.conf.get('spark.driver.memory'))
print("JVM options:", spark.conf.get('spark.driver.extraJavaOptions'))
```

**3. Useful JVM options for large memory:**

```python
jvm_options = "-Xmx48g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
```

## Complete Working Example:

```python
# Cell 1: Set environment (run this first)
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--driver-memory 48g --driver-max-result-size 16g --conf spark.driver.extraJavaOptions="-Xmx48g -XX:+UseG1GC" pyspark-shell'

# Cell 2: Create SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Cell 3: Verify
print(f"Driver memory: {spark.conf.get('spark.driver.memory')}")
print(f"JVM options: {spark.conf.get('spark.driver.extraJavaOptions')}")
```

Try Method 1 first - it's the most reliable for Jupyter environments. Make sure to restart your kernel completely before
running this.

