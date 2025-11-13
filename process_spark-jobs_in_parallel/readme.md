# Problem statement

I am running a PySpark job on AWS EMR.

The job reads all the configuration files into DataFrames, dictionaries, and an out_dir.
Based on this data, the model logic determines the type of iterations to perform. These iterations run in a specific
sequence — for example:
model_sequence = [2, 3, 4, 56, 22, 15, 6, 7, 9].

For each value n in model_sequence, the job performs a set of transformations using the DataFrames and dictionaries
loaded earlier. Once the transformation for a particular iteration is complete, the resulting DataFrame is written to
out_dir/iter_n.

I need help writing PySpark code so that each iteration in the model sequence runs in parallel, and the process should
not continue until all transformations and writes for the sequence have finished.

## Solution

To run each step in your `model_sequence` in parallel and ensure the process does not proceed until all model
transformations and writes are complete, you need to:

- Launch parallel jobs (one for each model value in `model_sequence`)
- Wait for all jobs to finish before your control flow continues

For PySpark on AWS EMR, the approach below uses Python's `concurrent.futures.ThreadPoolExecutor` to parallelize step
execution (each Spark job must be spawned from the driver), and future objects ensure synchronization. Note, you must
*not* share Spark DataFrames between threads; each thread should handle its own jobs independently.

Here’s an example pattern for your scenario:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Parallel Model Sequence").getOrCreate()

# Example: Pretend config/data loading
df_conf = spark.read.csv("config.csv")
config_dict = {"param1": 1, "param2": 2}
out_dir = "s3://bucket/model_outputs"
model_sequence = [2, 3, 4, 56, 22, 15, 6, 7, 9]


def perform_transformation(n, conf_df, conf_dict, out_dir):
    # --- Model-specific transformation logic here ---
    df = conf_df  # Replace with your logic, which depends on `n`
    # (Transform `df` based on config, dict, and model logic)
    transformed_df = df  # Example, put actual transformations here
    # Write output
    target = f"{out_dir}/iter_{n}"
    transformed_df.write.mode("overwrite").parquet(target)
    return n


# Thread pool for parallel execution (limit pool size based on your EMR/YARN capacity)
with ThreadPoolExecutor(max_workers=min(len(model_sequence), 8)) as executor:
    futures = []
    for n in model_sequence:
        # Submit each model iteration as a separate thread job
        futures.append(executor.submit(perform_transformation, n, df_conf, config_dict, out_dir))

    # Wait for all jobs to complete
    for future in as_completed(futures):
        res = future.result()  # Get return or raise exceptions, if any
        print(f"Model step {res} complete.")

print("All models transformations finished. Proceeding to next phase.")
```

### Notes:

- Do not modify shared DataFrames in threads; always process within individual tasks.
- Adjust `max_workers` to fit cluster capacity. If you have many executor cores, you can increase parallelism.
- The `perform_transformation` function should re-instantiate input DataFrames or read from sources per job to avoid
  thread conflicts.
- If transformation logic is expensive, consider `multiprocessing` instead of threads, but with Spark you typically
  trigger jobs from the driver.
- This pattern ensures that *all* steps are finished (written) before you proceed.

### Cluster Considerations

On EMR:

- If you need true cluster-side parallelism per model step, consider launching separate Spark applications per step (
  using EMR Steps API, or job orchestration tools), but for most logic, threaded driver-side submission suffices for
  moderate task concurrency.

This recipe ensures parallelism and collect-then-proceed semantics for your jobs.

***

**References**

- PySpark ThreadPool parallel patterns for IO jobs
- Practical EMR orchestration strategies

| Pattern    | Parallelism | Synchronization     | Caution                         |
|------------|-------------|---------------------|---------------------------------|
| ThreadPool | Yes         | as_completed/future | Don't share mutable DataFrames  |
| EMR Steps  | Yes         | Wait per step       | More setup, best for batch jobs |

## Thread safety of dataframe and dictionary

Yes, for thread safety in your parallel execution setup, you should clone or re-create data structures that may be
modified inside the `perform_transformation` function.

### DataFrame Handling

- P**ySpark DataFrames are inherently immutable;** transformations like `.select()`, `.withColumn()`, etc., do **not**
  modify the original DataFrame but return a new one.
- If your function only reads from the DataFrame or applies transformations that return new DataFrames, you do **not**
  need to clone the DataFrame for each thread.
- If you perform in-place modifications on Python objects (like Pandas DataFrames or local dicts), you must clone to
  prevent thread interference.

### Dictionary Handling

- **Python dictionaries are mutable;** if your logic **modifies** the dictionary contents during transformation, you must
  pass a *copy* to each thread:

```python
from copy import deepcopy

futures.append(
    executor.submit(perform_transformation, n, df_conf, deepcopy(config_dict), out_dir)
)
```

- If the dictionary is used **read-only**, cloning is not required.

### Recommended Practice

- For Spark DataFrames: Safe to share for read-only or transformation purposes.
- For Python dicts: Clone only if any thread might modify them.
- Never modify shared state in threads unless you use explicit synchronization mechanisms.

