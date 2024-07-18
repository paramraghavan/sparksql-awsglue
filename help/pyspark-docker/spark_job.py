from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Example Spark Job") \
        .getOrCreate()

    # Create a DataFrame with sample data
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Perform some transformations
    df_filtered = df.filter(df.Age > 30)

    # Show the result
    df_filtered.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()