# Setting Up PySpark in Docker on MacBook

This guide provides step-by-step instructions to install PySpark in a Docker container and run Spark jobs on a MacBook.

## Prerequisites
- Install Docker on your MacBook. You can download it from [Docker's official website](https://www.docker.com/products/docker-desktop).

## Step-by-Step Guide

### 1. Create a Dockerfile

First, create a new directory for your project and navigate into it. Inside this directory, create a file named `Dockerfile` with the following content:

```Dockerfile
# Use the official Spark image from Docker Hub
FROM bitnami/spark:latest

# Set environment variables
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Install PySpark and other dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install pyspark

# Set the working directory
WORKDIR /app

# Copy your application code to the container
COPY . /app

# Default command to run when container starts
CMD ["spark-submit", "your_spark_job.py"]

### Create Your Spark Job
In the same directory, create a Python file for your Spark job, for example, your_spark_job.py:
```python
from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

    # Example data
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

```

### Build Docker image
Open a terminal, navigate to your project directory, and build the Docker image:
```bash
docker build -t my-pyspark-app .

```

### Run the Docker Container
After building the Docker image, you can run the container to execute your Spark job:
```bash
docker run -it my-pyspark-app
```

#### Example Docker Run Command with Volume Mount
If you want to keep your code outside the Docker container and just mount it, you can run:
```bash
docker run -v $(pwd):/app -w /app -it my-pyspark-app

```
This command mounts the current directory (where your code is) to the /app directory inside the container and sets /app as the working directory.

### Additional Configuration
- Networking: If your Spark job needs network access, ensure the appropriate Docker network configurations are set.
- Spark Configurations: You can add more Spark configurations in the spark-submit command or through environment variables as needed.