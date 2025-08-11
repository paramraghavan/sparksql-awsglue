#!/usr/bin/env python3

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("CountryAgeAnalysis") \
        .getOrCreate()

    try:
        # Check if file path argument is provided
        if len(sys.argv) < 2:
            logger.error("Usage: python script.py <csv_file_path>")
            sys.exit(1)

        # Read CSV file
        readAsDF = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(sys.argv[1])

        # Repartition the DataFrame
        partitionedDF = readAsDF.repartition(2)

        # Apply transformations and aggregation
        countDF = partitionedDF \
            .where(col("Age") < 40) \
            .select("Age", "Gender", "Country", "state") \
            .groupBy("Country") \
            .count()

        # Collect results and log
        results = countDF.collect()
        logger.info("Results:")
        for row in results:
            logger.info(f"Country: {row['Country']}, Count: {row['count']}")

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        sys.exit(1)

    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()