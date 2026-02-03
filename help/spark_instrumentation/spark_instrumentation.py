"""
Spark Instrumentation Module
============================
Single import to add debugging/logging to all PySpark DataFrame operations.

USAGE:
    # Add this ONE line at the top of your auto-generated code:
    from spark_instrumentation import enable_instrumentation; enable_instrumentation()

LOGS GO TO:
    1. Console (stdout)
    2. File: ./spark_debug.log (configurable)

ENVIRONMENT VARIABLES:
    SPARK_DEBUG=1           - Auto-enable instrumentation on import
    SPARK_DEBUG_LOG=path    - Custom log file path
    SPARK_DEBUG_LEVEL=DEBUG - Log level (DEBUG, INFO, WARNING, ERROR)
"""

import os
import time
import functools
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logger(
    log_file: str = "spark_debug.log",
    level: int = logging.INFO,
    console: bool = True
) -> logging.Logger:
    """Configure logger with both file and console handlers"""
    
    logger = logging.getLogger("spark_debug")
    logger.setLevel(level)
    logger.handlers = []  # Clear existing handlers
    
    # Format with timestamp
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-5s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger


# Initialize logger with env var overrides
LOG_FILE = os.getenv("SPARK_DEBUG_LOG", "spark_debug.log")
LOG_LEVEL = getattr(logging, os.getenv("SPARK_DEBUG_LEVEL", "INFO").upper(), logging.INFO)
logger = setup_logger(log_file=LOG_FILE, level=LOG_LEVEL)


# ============================================================================
# INSTRUMENTATION STORAGE
# ============================================================================

_original_methods: Dict[str, Any] = {}
_operation_stats: List[Dict[str, Any]] = []
_instrumentation_enabled = False


# ============================================================================
# WRAPPER FUNCTIONS
# ============================================================================

def _instrument_transform(method_name: str):
    """Wrapper for transformation methods (lazy operations)"""
    from pyspark.sql import DataFrame
    
    original = getattr(DataFrame, method_name)
    _original_methods[method_name] = original
    
    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        start = time.time()
        
        # Format args for logging (truncate long values)
        def format_arg(a):
            s = str(a)
            return s[:50] + "..." if len(s) > 50 else s
        
        args_str = ", ".join(format_arg(a) for a in args[:3])
        if len(args) > 3:
            args_str += f", ... ({len(args)} total)"
        
        logger.info(f"▶ {method_name.upper()}({args_str})")
        logger.debug(f"  Input columns: {self.columns}")
        
        # Execute original method
        try:
            result = original(self, *args, **kwargs)
            elapsed = time.time() - start
            
            if isinstance(result, DataFrame):
                logger.info(f"  ✓ Output: {len(result.columns)} columns in {elapsed:.3f}s")
                logger.debug(f"  Output columns: {result.columns}")
                
                # Track operation history on the DataFrame
                history = getattr(self, '_spark_debug_history', []).copy()
                history.append({
                    "operation": method_name,
                    "time": elapsed,
                    "input_cols": len(self.columns),
                    "output_cols": len(result.columns),
                    "timestamp": datetime.now().isoformat()
                })
                result._spark_debug_history = history
                
                # Global stats
                _operation_stats.append({
                    "operation": method_name,
                    "time": elapsed,
                    "args": args_str
                })
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"  ✗ FAILED after {elapsed:.3f}s: {type(e).__name__}: {e}")
            raise
    
    return wrapper


def _instrument_action(method_name: str):
    """Wrapper for action methods (trigger computation)"""
    from pyspark.sql import DataFrame
    
    original = getattr(DataFrame, method_name)
    _original_methods[method_name] = original
    
    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        start = time.time()
        
        logger.info("=" * 60)
        logger.info(f"▶ ACTION: {method_name.upper()}")
        logger.info("=" * 60)
        
        # Show operation history
        history = getattr(self, '_spark_debug_history', [])
        if history:
            logger.info(f"  Operation chain ({len(history)} steps):")
            for i, h in enumerate(history, 1):
                logger.info(f"    {i}. {h['operation']} ({h['time']:.3f}s) → {h['output_cols']} cols")
        
        # Show schema
        logger.debug(f"  Schema:\n{self._jdf.schema().treeString()}")
        
        # Show execution plan
        try:
            plan = self._jdf.queryExecution().simpleString()
            # Truncate long plans
            if len(plan) > 500:
                plan = plan[:500] + "\n    ... (truncated)"
            logger.info(f"  Execution plan:\n    {plan}")
        except Exception as e:
            logger.debug(f"  Could not get execution plan: {e}")
        
        # Execute
        try:
            result = original(self, *args, **kwargs)
            elapsed = time.time() - start
            
            logger.info(f"  ✓ {method_name} completed in {elapsed:.3f}s")
            logger.info("=" * 60)
            
            _operation_stats.append({
                "operation": f"ACTION:{method_name}",
                "time": elapsed,
                "is_action": True
            })
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"  ✗ ACTION FAILED after {elapsed:.3f}s")
            logger.error(f"  Error: {type(e).__name__}: {e}")
            logger.info("=" * 60)
            raise
    
    return wrapper


def _instrument_write(method_name: str):
    """Wrapper for write operations"""
    from pyspark.sql import DataFrameWriter
    
    original = getattr(DataFrameWriter, method_name)
    _original_methods[f"writer_{method_name}"] = original
    
    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        start = time.time()
        logger.info(f"▶ WRITE.{method_name.upper()}()")
        logger.info(f"  Args: {args}, {kwargs}")
        
        try:
            result = original(self, *args, **kwargs)
            elapsed = time.time() - start
            logger.info(f"  ✓ Write completed in {elapsed:.3f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"  ✗ Write FAILED after {elapsed:.3f}s: {e}")
            raise
    
    return wrapper


# ============================================================================
# HELPER METHODS ADDED TO DATAFRAME
# ============================================================================

def _df_debug_schema(self):
    """Print DataFrame schema"""
    logger.info(f"Schema:\n{self._jdf.schema().treeString()}")
    return self


def _df_debug_plan(self):
    """Print execution plan"""
    logger.info(f"Logical Plan:\n{self._jdf.queryExecution().logical().toString()}")
    logger.info(f"Physical Plan:\n{self._jdf.queryExecution().executedPlan().toString()}")
    return self


def _df_debug_sample(self, n: int = 5):
    """Show sample rows without triggering full action logging"""
    logger.info(f"Sample ({n} rows):")
    try:
        pdf = self.limit(n).toPandas()
        logger.info(f"\n{pdf.to_string()}")
    except Exception as e:
        logger.warning(f"Could not show sample: {e}")
    return self


def _df_debug_summary(self):
    """Print comprehensive debug summary"""
    logger.info("=" * 60)
    logger.info("DATAFRAME DEBUG SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Columns ({len(self.columns)}): {self.columns}")
    
    history = getattr(self, '_spark_debug_history', [])
    if history:
        total_time = sum(h['time'] for h in history)
        logger.info(f"Transformations: {len(history)}, Total time: {total_time:.3f}s")
        for i, h in enumerate(history, 1):
            logger.info(f"  {i}. {h['operation']} ({h['time']:.3f}s)")
    
    logger.info("=" * 60)
    return self


def _df_debug_checkpoint(self, name: str = "checkpoint"):
    """Mark a checkpoint in the log for easier debugging"""
    logger.info(f"📍 CHECKPOINT: {name}")
    logger.info(f"   Columns: {self.columns}")
    history = getattr(self, '_spark_debug_history', [])
    logger.info(f"   Operations so far: {len(history)}")
    return self


# ============================================================================
# MAIN ENABLE/DISABLE FUNCTIONS
# ============================================================================

# Methods to instrument
TRANSFORM_METHODS = [
    'select', 'selectExpr', 'filter', 'where', 'groupBy', 'agg',
    'join', 'crossJoin', 'withColumn', 'withColumnRenamed', 'drop',
    'distinct', 'dropDuplicates', 'orderBy', 'sort', 'limit',
    'union', 'unionAll', 'unionByName', 'intersect', 'subtract',
    'pivot', 'alias', 'coalesce', 'repartition', 'cache', 'persist',
    'unpersist', 'sample', 'randomSplit', 'fillna', 'replace'
]

ACTION_METHODS = [
    'count', 'collect', 'show', 'take', 'first', 'head', 'tail',
    'foreach', 'foreachPartition', 'toPandas', 'toLocalIterator'
]

WRITE_METHODS = ['save', 'saveAsTable', 'insertInto', 'jdbc', 'json', 'parquet', 'csv', 'orc']


def enable_instrumentation(log_file: Optional[str] = None, level: int = logging.INFO):
    """
    Enable DataFrame instrumentation.
    
    Args:
        log_file: Path to log file (default: spark_debug.log)
        level: Logging level (default: INFO)
    """
    global _instrumentation_enabled, logger, LOG_FILE
    
    if _instrumentation_enabled:
        logger.warning("Instrumentation already enabled")
        return
    
    # Reconfigure logger if custom settings provided
    if log_file:
        LOG_FILE = log_file
        logger = setup_logger(log_file=log_file, level=level)
    
    from pyspark.sql import DataFrame, DataFrameWriter
    
    logger.info("🔧 SPARK INSTRUMENTATION ENABLED")
    logger.info(f"   Log file: {LOG_FILE}")
    logger.info(f"   Log level: {logging.getLevelName(level)}")
    
    # Instrument transform methods
    for method in TRANSFORM_METHODS:
        if hasattr(DataFrame, method):
            try:
                setattr(DataFrame, method, _instrument_transform(method))
            except Exception as e:
                logger.debug(f"Could not instrument {method}: {e}")
    
    # Instrument action methods
    for method in ACTION_METHODS:
        if hasattr(DataFrame, method):
            try:
                setattr(DataFrame, method, _instrument_action(method))
            except Exception as e:
                logger.debug(f"Could not instrument {method}: {e}")
    
    # Instrument write methods
    for method in WRITE_METHODS:
        if hasattr(DataFrameWriter, method):
            try:
                setattr(DataFrameWriter, method, _instrument_write(method))
            except Exception as e:
                logger.debug(f"Could not instrument writer.{method}: {e}")
    
    # Add helper methods to DataFrame
    DataFrame.debug_schema = _df_debug_schema
    DataFrame.debug_plan = _df_debug_plan
    DataFrame.debug_sample = _df_debug_sample
    DataFrame.debug_summary = _df_debug_summary
    DataFrame.debug_checkpoint = _df_debug_checkpoint
    
    _instrumentation_enabled = True
    logger.info("   Ready! All DataFrame operations will be logged.")
    logger.info("-" * 60)


def disable_instrumentation():
    """Restore original DataFrame methods"""
    global _instrumentation_enabled
    
    if not _instrumentation_enabled:
        logger.warning("Instrumentation not enabled")
        return
    
    from pyspark.sql import DataFrame, DataFrameWriter
    
    # Restore original methods
    for method, original in _original_methods.items():
        if method.startswith("writer_"):
            setattr(DataFrameWriter, method[7:], original)
        else:
            setattr(DataFrame, method, original)
    
    _instrumentation_enabled = False
    logger.info("🔧 SPARK INSTRUMENTATION DISABLED")


def get_stats() -> List[Dict[str, Any]]:
    """Get all recorded operation statistics"""
    return _operation_stats.copy()


def print_stats_summary():
    """Print summary of all operations"""
    if not _operation_stats:
        logger.info("No operations recorded")
        return
    
    logger.info("=" * 60)
    logger.info("OPERATION STATISTICS SUMMARY")
    logger.info("=" * 60)
    
    total_time = sum(s['time'] for s in _operation_stats)
    actions = [s for s in _operation_stats if s.get('is_action')]
    transforms = [s for s in _operation_stats if not s.get('is_action')]
    
    logger.info(f"Total operations: {len(_operation_stats)}")
    logger.info(f"  Transformations: {len(transforms)}")
    logger.info(f"  Actions: {len(actions)}")
    logger.info(f"Total time: {total_time:.3f}s")
    
    # Top 5 slowest operations
    sorted_ops = sorted(_operation_stats, key=lambda x: x['time'], reverse=True)[:5]
    logger.info("Top 5 slowest operations:")
    for op in sorted_ops:
        logger.info(f"  - {op['operation']}: {op['time']:.3f}s")
    
    logger.info("=" * 60)


def clear_stats():
    """Clear recorded statistics"""
    global _operation_stats
    _operation_stats = []
    logger.info("Statistics cleared")


# ============================================================================
# AUTO-ENABLE VIA ENVIRONMENT VARIABLE
# ============================================================================

if os.getenv("SPARK_DEBUG", "0") == "1":
    enable_instrumentation()
