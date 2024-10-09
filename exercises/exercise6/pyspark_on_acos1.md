I understand you're encountering an error when trying to run PySpark. The error suggests that PySpark is unable to find
the necessary Spark files. This is likely due to incorrect path settings or a mismatch between your Spark installation
and the paths PySpark is looking for. Let's troubleshoot this step by step:

1. Verify Spark Installation:
   First, let's check if Spark is properly installed:

   ```
   brew info apache-spark
   ```

   This should show you the installed version and path. If it's not installed, you'll need to install it:

   ```
   brew install apache-spark
   ```

2. Check Spark Version:
   The error message mentions version 3.5.0, but let's confirm the actual version installed:

   ```
   brew list apache-spark --versions
   ```

3. Update Environment Variables:
   Based on the error, it seems the paths are not set correctly. Add or update these lines in your `~/.zshrc` or
   `~/.bash_profile`:

   ```
   export SPARK_HOME=$(brew --prefix apache-spark)/libexec
   export PATH="$SPARK_HOME/bin:$PATH"
   export PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"
   ```

   After adding these, run:

   ```
   source ~/.zshrc  # or source ~/.bash_profile if you use bash
   ```

4. Verify Paths:
   Check if the Spark files exist:

   ```
   ls $SPARK_HOME/bin/spark-submit
   ls $SPARK_HOME/bin/load-spark-env.sh
   ```

   If these files don't exist, there might be an issue with your Spark installation.

5. Check Python Version:
   The error message mentions Python 3.12. Ensure that this is the correct version you want to use with PySpark. Some
   versions of PySpark might not be fully compatible with the latest Python versions. You might want to try with Python
   3.9 or 3.10 if possible.

6. Reinstall PySpark:
   If you're using a virtual environment (which seems to be the case based on the error message), try reinstalling
   PySpark:

   ```
   pip uninstall pyspark
   pip install pyspark
   ```

7. Verify PySpark Installation:
   After reinstalling, try to import PySpark in Python:

   ```python
   python3
   >>> import pyspark
   >>> print(pyspark.__version__)
   ```

   If this works, it means PySpark is installed correctly.

8. Run PySpark Directly:
   Try running PySpark directly from the command line:

   ```
   $SPARK_HOME/bin/pyspark
   ```

   If this works, the issue might be with how PyCharm is configured to run PySpark.

9. PyCharm Configuration:
   If you're using PyCharm, make sure your project interpreter is set correctly:
    - Go to PyCharm > Preferences > Project: YourProject > Python Interpreter
    - Select the correct Python interpreter (the one where PySpark is installed)
    - In your run configuration, make sure the environment variables are set correctly, especially `SPARK_HOME`

If after trying these steps you're still encountering issues, please provide the following information:

1. The output of `brew info apache-spark`
2. The content of your `~/.zshrc` or `~/.bash_profile` file (the part related to Spark and Python)
3. The output of `echo $SPARK_HOME`
4. The Python version you're using (`python --version`)

With this additional information, I can provide more targeted assistance to resolve the PySpark setup issue.