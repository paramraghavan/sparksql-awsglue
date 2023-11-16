# install java
- install java
- Check if Java is installed by running java -version.
- If Java is not installed, download and install it from the Oracle website or use a package manager like Homebrew with the command **brew install java**.
- add JAVA_HOME to your shell profile file
```sh
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-11.jdk/Contents/Home
```  
# install spark
- brew install apache-spark
- brew info apache-spark, will give the path for the spark installed
- brew install wget
- wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.349/aws-java-sdk-bundle-1.12.349.jar
- wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
- In my case spark got installed at /opt/homebrew/Cellar/apache-spark/3.4.1
- **Note:** as apache spark 3.4.1 is installed, install pyspark-3.4.1 in your virtual environment as well.
- pip install pyspark==3.4.1
# Set Up Environment Variables:
- For Spark to work correctly, you need to set up a few environment variables. You can do this in your shell profile file (e.g., ~/.bash_profile for Bash or ~/.zshrc for Zsh).
```sh
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.5.0/libexec
export PATH=$SPARK_HOME/bin:$PATH
```
Replace /usr/local/Cellar/apache-spark/3.5.0/libexec with the actual path where Spark is installed on your system. This path can vary depending on the Spark version you have installed.

# copy aws sdk and hadoop
- Copy aws-java-sdk-bundle version 1.12.349.jar and hadoop-aws version 3.3.1.jar to /opt/homebrew/Cellar/apache-spark/3.4.1/libexec/jars
- hadoop canbe installed via - **brew install hadoop**.

# Verify install
- run command -  spark-shell or pyspark

  
# install jupyter notebook
- python3 -m pip install notebook
- or python3 -m pip install jupyterlab
- Add the folowing to the env variables
```
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
```  
# Run the following in jupyter notebook
  
```python
from pyspark.sql import SparkSession
import os
import sys
# Path for spark source folder
# replace praghavan with you own home directory
os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/3.5.0/libexec"
# Append pyspark  to Python Path
sys.path.append("/usr/local/Cellar/apache-spark/3.5.0/libexec/python/pyspark")

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
# Debug
Add the following to the main method and use the spark session returned by local_spark method in you code:
```python
def local_spark():
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
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                         "com.amazonaws.auth.profile.ProfileCredentialsProvider")

    return spark


if __name__ == "__main__":
    # arg_parser = sp.util.get_standard_date_arg_parser_with_bucket_prefix()
    # args = arg_parser.parse_known_args()[0]
    # job = sp.glue.ArgParseBehavior(job_inst=CurateSpamTraps, arg=args, sys_ars=sys.argv).get_job_instance()
    spark = local_spark()
```

### Ref
- https://www.jitsejan.com/using-pyspark-with-s3-updated
