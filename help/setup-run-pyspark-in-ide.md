# Setting Up Spark and PySpark with PyCharm

I'll guide you through setting up Spark on Windows or Mac, and configuring PyCharm to run PySpark code.

## Installing Spark on Windows

1. **Install Java JDK** (Spark requires Java)
   - Download and install the latest Java JDK from Oracle's website
   - Set JAVA_HOME environment variable to your JDK installation path

2. **Install Spark**
   - Download Apache Spark from the [official website](https://spark.apache.org/downloads.html)
   - Extract the downloaded .tgz file (use 7-Zip or similar tool)
   - Move the extracted folder to a location like `C:\spark`
   - Add the following environment variables:
     - SPARK_HOME: `C:\spark`
     - Add `%SPARK_HOME%\bin` to your PATH

3. **Install Python**
   - Install Python 3.x if you haven't already
   - Ensure pip is installed

## Installing Spark on Mac

1. **Install Java JDK**
   - Install Java using Homebrew: `brew install --cask java`
   - Or download from Oracle's website

2. **Install Spark using Homebrew**
   ```bash
   brew install apache-spark
   ```

3. **Set environment variables** (in ~/.bash_profile or ~/.zshrc)
   ```bash
   export SPARK_HOME=/usr/local/Cellar/apache-spark/[version]/libexec
   export PATH=$PATH:$SPARK_HOME/bin
   ```

## Install PySpark

For both Windows and Mac:

```bash
pip install pyspark
```

## Configure PyCharm for PySpark

1. **Open PyCharm and Create a New Project**
   - Choose your Python interpreter

2. **Set Project Interpreter**
   - Go to File → Settings → Project → Python Interpreter
   - Click + to install packages and add pyspark

3. **Configure Environment Variables in PyCharm**
   - Go to Run → Edit Configurations
   - Click + to add a new configuration and select Python
   - Add the following environment variables:
     - PYSPARK_PYTHON: Path to your Python executable
     - SPARK_HOME: Path to your Spark installation

4. **Create a Simple PySpark Script**

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .master("local[*]") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Java", 20000), ("Python", 25000), ("Scala", 15000)]
columns = ["Language", "Users"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
```

5. **Run the Script**
   - Right-click on your script in PyCharm and select "Run"

## Troubleshooting Tips

- If you encounter "winutils.exe" errors on Windows, download winutils.exe for your Hadoop version and place it in `%SPARK_HOME%\bin`
- For Mac permission issues, run `chmod +x` on the Spark bin files
- Verify Java is properly installed: `java -version`
- Check Spark installation: `spark-shell --version`

## How to get the right winutils version
When you encounter "winutils.exe" errors on Windows, you need to download the correct version that matches your Hadoop version. Here's how to identify which version you need:

1. **Check your Spark's Hadoop version**:
   - Open a command prompt and run: `spark-shell --version`
   - Look for a line like "Using Hadoop X.X.X" in the output
   - Alternatively, check the folder name of your Spark installation, which often includes the Hadoop version (e.g., "spark-3.4.0-bin-hadoop3")

2. **Find the Hadoop version in PySpark code**:
   ```python
   from pyspark.sql import SparkSession
   
   spark = SparkSession.builder.getOrCreate()
   hadoop_version = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
   print(f"Hadoop version: {hadoop_version}")
   ```

3. **Check in the Spark properties file**:
   - Look in `%SPARK_HOME%\conf\spark-defaults.conf` for Hadoop-related properties
   - Or check the release notes/README in your Spark installation folder

Once you've identified your Hadoop version, download the corresponding winutils.exe:

1. Find a trusted repository for winutils, such as:
   - https://github.com/steveloughran/winutils (official but not always up-to-date)
   - https://github.com/cdarlint/winutils (community-maintained)

2. Navigate to the folder matching your Hadoop version (e.g., "hadoop-3.2.0")

3. Download the winutils.exe file

4. Place it in your `%SPARK_HOME%\bin` directory

5. You may also need to create a `\tmp\hive` directory and grant permissions:
   ```
   mkdir C:\tmp\hive
   %SPARK_HOME%\bin\winutils.exe chmod -R 777 C:\tmp\hive
   ```

If you're still having issues, you can also try a workaround by setting this environment variable:
```
set HADOOP_HOME=%SPARK_HOME%
```
This will help PySpark find the winutils.exe file in your Spark installation directory.

