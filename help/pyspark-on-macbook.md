# Steps
- brew install apache-spark
- brew info apache-spark
- brew install wget
- wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.349/aws-java-sdk-bundle-1.12.349.jar
- wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
- In my case spark got installed at /opt/homebrew/Cellar/apache-spark/3.4.1
# Next step
- Copy aws-java-sdk-bundle version 1.12.349.jar and hadoop-aws version 3.3.1.jar to /opt/homebrew/Cellar/apache-spark/3.4.1/libexec/jars
- Run the following in jupyter notebook
  
```python:

  from pyspark.sql import SparkSession
  
  import os
  import sys
  # Path for spark source folder
  # replace praghavan with you own home directory
  os.environ['SPARK_HOME'] = "/opt/homebrew/Cellar/apache-spark/3.4.1/libexec"
  # Append pyspark  to Python Path
  sys.path.append("/opt/homebrew/Cellar/apache-spark/3.4.1/libexec/python/pyspark")
  
  # Set profile to be used by the credentials provider
  os.environ["AWS_PROFILE"] = "prd"
  # Create Spark Session
  spark = SparkSession.builder.getOrCreate()
  # Make sure the ProfileCredentialsProvider is used to authenticate in Spark
  spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
  #Validate the code
  S3_URI = "s3a://some-bucket-with-parquet-files/"
  df = spark.read.parquet(S3_URI)
  df.take(5)
``` 

### Ref
- https://www.jitsejan.com/using-pyspark-with-s3-updated
