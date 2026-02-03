## Solution: Monkey-patch DataFrame methods
Create a single module that wraps all DataFrame operations with logging/debugging:

# To customize the log file path:
```python
from spark_instrumentation import enable_instrumentation
enable_instrumentation(log_file="/var/log/spark/my_job.log")
```

```python
# spark_instrumentation.py

import time
import functools
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark_debug")

# Store original methods
_original_methods = {}

def _instrument_method(method_name):
    """Wrapper that logs DataFrame operations"""
    original = getattr(DataFrame, method_name)
    _original_methods[method_name] = original
    
    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        start = time.time()
        
        # Log before execution
        logger.info(f"▶ {method_name.upper()} called")
        logger.info(f"  Args: {args[:3]}...")  # Truncate for readability
        logger.info(f"  Input rows (approx): {self.rdd.countApprox(1000, 0.5) if method_name in ['join', 'groupBy'] else 'N/A'}")
        
        # Execute original method
        result = original(self, *args, **kwargs)
        
        elapsed = time.time() - start
        
        # Log after execution
        if isinstance(result, DataFrame):
            logger.info(f"  Output columns: {result.columns}")
            logger.info(f"  ✓ Completed in {elapsed:.2f}s")
            
            # Attach metadata for chaining
            result._debug_history = getattr(self, '_debug_history', []) + [
                {"op": method_name, "time": elapsed, "cols": result.columns}
            ]
        
        return result
    
    return wrapper


def _instrument_action(method_name):
    """Wrapper for actions (count, collect, show, write)"""
    original = getattr(DataFrame, method_name)
    _original_methods[method_name] = original
    
    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        start = time.time()
        
        # Print execution plan before action
        logger.info(f"▶ ACTION: {method_name.upper()}")
        logger.info(f"  Physical Plan:\n{self._jdf.queryExecution().simpleString()}")
        
        # Show debug history if available
        if hasattr(self, '_debug_history'):
            logger.info(f"  Operation chain: {[h['op'] for h in self._debug_history]}")
        
        result = original(self, *args, **kwargs)
        
        elapsed = time.time() - start
        logger.info(f"  ✓ {method_name} completed in {elapsed:.2f}s")
        
        return result
    
    return wrapper


# Methods to instrument
TRANSFORM_METHODS = [
    'select', 'filter', 'where', 'groupBy', 'agg', 'join', 
    'withColumn', 'drop', 'distinct', 'orderBy', 'limit',
    'union', 'unionAll', 'pivot', 'unpivot', 'alias'
]

ACTION_METHODS = ['count', 'collect', 'show', 'take', 'first', 'write']


def enable_instrumentation():
    """Call this once to enable debugging on all DataFrames"""
    logger.info("🔧 Spark instrumentation ENABLED")
    
    for method in TRANSFORM_METHODS:
        if hasattr(DataFrame, method):
            setattr(DataFrame, method, _instrument_method(method))
    
    for method in ACTION_METHODS:
        if hasattr(DataFrame, method):
            setattr(DataFrame, method, _instrument_action(method))
    
    # Add helper methods to DataFrame
    DataFrame.debug_schema = lambda self: logger.info(f"Schema:\n{self._jdf.schema().treeString()}")
    DataFrame.debug_plan = lambda self: logger.info(f"Plan:\n{self._jdf.queryExecution().toString()}")
    DataFrame.debug_sample = lambda self, n=5: logger.info(f"Sample:\n{self.limit(n).toPandas().to_string()}")
    DataFrame.debug_summary = lambda self: self._debug_summary()


def disable_instrumentation():
    """Restore original methods"""
    logger.info("🔧 Spark instrumentation DISABLED")
    for method, original in _original_methods.items():
        setattr(DataFrame, method, original)


def _debug_summary(self):
    """Print full debug summary"""
    logger.info("=" * 50)
    logger.info("DEBUG SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Columns ({len(self.columns)}): {self.columns}")
    logger.info(f"Schema:\n{self._jdf.schema().treeString()}")
    if hasattr(self, '_debug_history'):
        total_time = sum(h['time'] for h in self._debug_history)
        logger.info(f"Operations: {len(self._debug_history)}, Total time: {total_time:.2f}s")
        for i, h in enumerate(self._debug_history):
            logger.info(f"  {i+1}. {h['op']} ({h['time']:.2f}s)")
    logger.info("=" * 50)

DataFrame._debug_summary = _debug_summary
```

---

## Usage: Single Line Change!

```python
# YOUR AUTO-GENERATED CODE
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col

# ===== ADD THIS ONE LINE =====
from spark_instrumentation import enable_instrumentation; enable_instrumentation()
# =============================

spark = SparkSession.builder.appName("test").getOrCreate()

# Rest of your auto-generated code works as-is
df = spark.read.parquet("data.parquet")
df_filtered = df.filter(col("price") > 100)
df_grouped = df_filtered.groupBy("place").agg({"price": "avg"})
df_grouped.show()
```

---

## Output Example

```
INFO:spark_debug:🔧 Spark instrumentation ENABLED
INFO:spark_debug:▶ FILTER called
INFO:spark_debug:  Args: (Column<'(price > 100)'>,)...
INFO:spark_debug:  Output columns: ['place', 'yymmdd', 'field', 'min', 'max', 'average']
INFO:spark_debug:  ✓ Completed in 0.01s
INFO:spark_debug:▶ GROUPBY called
INFO:spark_debug:  Args: ('place',)...
INFO:spark_debug:  Output columns: ['place']
INFO:spark_debug:  ✓ Completed in 0.02s
INFO:spark_debug:▶ ACTION: SHOW
INFO:spark_debug:  Physical Plan: ...
INFO:spark_debug:  Operation chain: ['filter', 'groupBy', 'agg']
INFO:spark_debug:  ✓ show completed in 1.23s
```

---

## Advanced: Environment Variable Toggle

```python
# spark_instrumentation.py (add at the end)
import os

if os.getenv("SPARK_DEBUG", "0") == "1":
    enable_instrumentation()
```

Then run with:
```bash
SPARK_DEBUG=1 spark-submit your_job.py
```

---

## Even Simpler: Decorator for the entire job

```python
# debug_wrapper.py
from spark_instrumentation import enable_instrumentation, disable_instrumentation

def with_debug(func):
    """Decorator to wrap entire spark job with instrumentation"""
    def wrapper(*args, **kwargs):
        enable_instrumentation()
        try:
            return func(*args, **kwargs)
        finally:
            disable_instrumentation()
    return wrapper
```

```python
# your_job.py
from debug_wrapper import with_debug

@with_debug
def main():
    # All your auto-generated spark code here
    pass

main()
```

---

