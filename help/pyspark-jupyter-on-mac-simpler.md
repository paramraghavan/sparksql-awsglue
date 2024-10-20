# Simple steps to setup PySpark and Jupyter Notebook on your MacBook with Python 3.12 and OpenJDK 17:

1. Install OpenJDK 17:
   ```shell
   brew install openjdk@17
   ```
2. Set JAVA_HOME:
   Add to ~/.zshrc or ~/.bash_profile:
  >> for my setup used .zshrc
   
  ```shell
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   export PATH=$JAVA_HOME/bin:$PATH
   ```
3. Install Python 3.12 (if not already installed):

   ```shell
   #brew install python@3.12
   brew install python@3.11
   ```
4. Create a virtual environment:

   ```shell
   #python3.12 -m venv pyspark_env
   python3.11 -m venv pyspark_env
   source pyspark_env/bin/activate
   ```
5. Install PySpark and Jupyter:

   ```shell
   pip install pyspark jupyter
   ```
6. Set Spark environment variables:
   Add to ~/.zshrc or ~/.bash_profile:

   ```shell
   export SPARK_HOME=/Users/`whoami`/pyspark_env/lib/python3.10/site-packages/pyspark
   #export SPARK_HOME=$(pip3 show pyspark | grep Location | cut -d' ' -f2)/pyspark
   export PATH=$SPARK_HOME/bin:$PATH
   export PYSPARK_PYTHON=python3.10
   export PYSPARK_DRIVER_PYTHON=python3.10
   #export PYSPARK_DRIVER_PYTHON=jupyter
   export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
   ```
7. Start Jupyter with PySpark:

   ```shell
   pyspark
   ```
## Error
Get error indicates a mismatch between the Python versions used by the PySpark driver and worker. Here's what's happening:

The PySpark driver is using Python 3.10
The PySpark worker is using Python 3.12
> Following fixed it
>  export PYSPARK_PYTHON=python3.10
   export PYSPARK_DRIVER_PYTHON=python3.10

**In a new notebook, you can test your PySpark setup with:**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(spark.version)
```

Remember to activate your virtual environment (`source ~/pyspark_env/bin/activate`) before starting Jupyter Notebook
each time you want to use PySpark.
