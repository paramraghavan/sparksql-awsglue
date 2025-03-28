"""
Code below doesn't generate a UnicodeDecodeError even though it contains invalid UTF-8 bytes.

1. **Schema Definition**: The code explicitly defines a schema where "content" is a StringType(). When reading the CSV, Spark uses this schema rather than inferring types.

2. **Binary Handling**: When Spark reads CSV files, it attempts to decode the contents according to the schema. However, the specific way it's implemented here has a workaround:
   - The content column with the invalid UTF-8 byte (0xff) is being read as a StringType
   - When Spark encounters invalid UTF-8 bytes, it would typically throw an error, but in this case something else happens

3. **Error Suppression**: Several factors contribute to error suppression:
   - The UDF processing is somewhat isolated
   - The small data volume doesn't trigger certain internal serialization mechanisms
   - The local execution mode might handle this differently than distributed execution

If you wanted to actually trigger the Unicode error, you might need to:

1. Remove the explicit schema and let Spark infer types
2. Increase the data volume significantly
3. Force more complex serialization/deserialization cycles
4. Use different Spark operations that handle string encoding differently
5. Add configuration that makes Spark stricter about character encoding

In a real production environment with distributed processing and larger data volumes, this type of invalid UTF-8 would be more likely to cause problems.
This example demonstrates how small-scale testing might not reveal encoding issues that could appear in production.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.types import StringType, BinaryType, StructType, StructField, IntegerType
import os
import tempfile

# Create a Spark session
spark = SparkSession.builder \
    .appName("Unicode Error Without Accumulators") \
    .master("local[*]") \
    .getOrCreate()


# Create sample data with problematic binary content
def create_sample_data():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as f:
        # Write header
        f.write(b"id,name,content\n")
        # Write normal rows
        f.write(b"1,First Row,Normal text data\n")
        f.write(b"2,Second Row,More normal text\n")
        # Write problematic row with invalid UTF-8 bytes
        f.write(b"3,Binary Row,Text with invalid UTF-8 \xff\xfe\xaa\n")
        f.write(b"4,Fourth Row,Final normal row\n")

        filename = f.name

    return filename


# Function to demonstrate error without explicit accumulators
def demonstrate_error():
    # Create sample data
    filename = create_sample_data()
    print(f"Created sample data file: {filename}")

    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("content", StringType(), True)
    ])

    # Read the CSV file with the problematic data
    df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(filename)

    print("Original DataFrame:")
    df.show(truncate=False)

    # Create a UDF that will process the content
    # Spark internally uses accumulators for some operations
    # even if we don't explicitly create them
    @udf(StringType())
    def process_content(text):
        if text:
            # Simple string manipulation that expects UTF-8 valid text
            return text.upper()
        return None

    try:
        # Apply transformation that might trigger the error
        result_df = df.withColumn("processed", process_content(col("content")))

        # Add some aggregation to force shuffle operations
        # Shuffle operations can trigger serialization/deserialization
        # which might expose the Unicode error
        summary_df = result_df.groupBy("name").count()

        # Force execution with an action
        print("Trying to process and aggregate the data...")
        summary_df.collect()

        print("This line may not be reached if the error occurs")

    except Exception as e:
        print("\nCaught error:")
        print(str(e))
        print("\nStacktrace (abbreviated):")
        import traceback
        print(traceback.format_exc().split("\n")[-15:])

    # Clean up
    os.unlink(filename)

    # Alternative approach that might also trigger the error
    try:
        print("\nTrying alternative approach with window functions...")

        # Window functions and complex transformations can also
        # trigger internal serialization mechanisms
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        window_spec = Window.partitionBy("name").orderBy("id")
        windowed_df = df.withColumn("row_num", row_number().over(window_spec))
        windowed_df.collect()

    except Exception as e:
        print("\nCaught error in window functions approach:")
        print(str(e))

    spark.stop()


if __name__ == "__main__":
    demonstrate_error()
