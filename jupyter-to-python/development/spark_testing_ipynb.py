# Testing Spark Partitioning Strategies
# Purpose: Find optimal partitioning for our datasets
"""
This notebook focuses on testing infrastructure configurations rather than data analysis. It helps optimize the technical
environment and would inform how production code is configured. The findings might be implemented in configuration files or deployment scripts.
"""
import sys

sys.path.append("../../")

from src.utils.spark_utils import create_spark_session, time_execution
from src.utils.aws_utils import get_s3_file_size

# Create testing session with custom logging
spark = create_spark_session(
    "PartitionTest",
    {"spark.sql.shuffle.partitions": "200"}
)

# Load large test dataset
df = spark.read.parquet("s3://my-bucket/data/raw/large_dataset.parquet")
print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# Test different partitioning strategies and measure performance
results = []

for num_partitions in [50, 100, 200, 500]:
    # Repartition data
    df_test = df.repartition(num_partitions)

    # Run test query with timing
    time_taken = time_execution(lambda: df_test.groupBy("customer_id").count().collect())

    results.append({
        "partitions": num_partitions,
        "time_seconds": time_taken
    })

# Print results
for result in results:
    print(f"Partitions: {result['partitions']}, Time: {result['time_seconds']:.2f}s")

# Create recommended configuration
optimal = min(results, key=lambda x: x['time_seconds'])
print(f"Recommended spark.sql.shuffle.partitions: {optimal['partitions']}")