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
   brew install python@3.12
   ```
4. Create a virtual environment:

   ```shell
   python3.12 -m venv pyspark_env
   source pyspark_env/bin/activate
   ```
5. Install PySpark and Jupyter:

   ```shell
   pip install pyspark jupyter
   ```
6. Set Spark environment variables:
   Add to ~/.zshrc or ~/.bash_profile:

   ```shell
   export SPARK_HOME=$(pip show pyspark | grep Location | cut -d' ' -f2)/pyspark
   export PATH=$SPARK_HOME/bin:$PATH
   export PYSPARK_PYTHON=python3.12
   export PYSPARK_DRIVER_PYTHON=jupyter
   export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
   ```
7. Start Jupyter with PySpark:

   ```shell
   pyspark
   ```

