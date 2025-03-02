When running Spark on your desktop (not on EMR), you need to configure it properly to access S3 files.

## Setting Up Spark on Your Desktop to Access S3

### 1. AWS Credentials Setup

First, you need to provide AWS credentials to Spark:

```python
import os
from pyspark.sql import SparkSession

# Method 1: Set AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_key'
os.environ['AWS_SESSION_TOKEN'] = 'your_session_token'  # if using temporary credentials

# Method 2: Create Spark session with AWS credentials in configuration
spark = SparkSession.builder
.appName("S3 File Access")
.config("spark.hadoop.fs.s3a.access.key", "your_access_key")
.config("spark.hadoop.fs.s3a.secret.key", "your_secret_key")
.config("spark.hadoop.fs.s3a.session.token", "your_session_token") \  # if needed
.getOrCreate()
```

### 2. Add AWS Hadoop Connector JAR Files

You need the S3A connector libraries:

```python
# Add AWS Hadoop libraries when creating the Spark session
spark = SparkSession.builder
.appName("S3 File Access")
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
.getOrCreate()
```

### 3. Use S3A Protocol
When running Spark locally and accessing S3, always use the S3A protocol for best performance, reliability, and security.
When reading files, use the "s3a://" protocol instead of "s3://":

```python
# Reading from S3 using the s3a protocol
df = spark.read.parquet("s3a://your-bucket/your-path/")
```

### 4. Example: Complete Code

Here's a complete working example:

```python
from pyspark.sql import SparkSession

# Create Spark session with S3 access
spark = SparkSession.builder
.appName("S3 File Access")
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
.config("spark.hadoop.fs.s3a.access.key", "your_access_key")
.config("spark.hadoop.fs.s3a.secret.key", "your_secret_key")
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
.getOrCreate()

# Read data from S3
df = spark.read.parquet("s3a://your-bucket/your-path/")
df.show(5)
```

### Alternative: AWS CLI Credentials

If you've already configured AWS CLI on your machine, Spark can use those credentials:

```python
spark = SparkSession.builder
.appName("S3 File Access")
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
.config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
.getOrCreate()
```

## Using AWS profile

I'll update the code to use an AWS profile for S3 access from your desktop:

```python
from pyspark.sql import SparkSession

# Create Spark session with S3 access using a specific AWS profile
spark = SparkSession.builder
    .appName("S3 File Access")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.profile", "your_profile_name") \  # Specify your AWS profile name here
.getOrCreate()

# Read data from S3
df = spark.read.parquet("s3a://your-bucket/your-path/")
df.show(5)
```

If you have multiple profiles set up in your `~/.aws/credentials` file, you can specify which profile to use with the
`spark.hadoop.fs.s3a.profile` configuration parameter.

When using a named profile, make sure:

1. Your profile is correctly set up in `~/.aws/credentials`
2. The profile has the necessary permissions to access the S3 buckets
3. Replace `"your_profile_name"` with the actual name of your AWS profile


### Performance Considerations

When running on your desktop (not EMR):

- You'll be limited by your internet connection speed
- Large files will take longer to download
- Consider filtering or sampling data when possible
- Processing will use your local machine's resources, not a cluster

This setup allows your local Spark installation to read directly from S3 without needing to first download the files.