## Build config driven pyspark, use json for rules this code parses a nested json, example of nested json attached

* Here we create_schema function that recursively creates a Spark schema based on the configuration.
* The parse_json function takes the Spark session, JSON data, and configuration as input. It creates a DataFrame from
  the JSON data and then applies the parsing rules defined in the configuration.
* For each field in the configuration, it uses the col function to extract the value from the specified path.
* For array fields, it uses explode and from_json to flatten the nested structure.

```shell
brew install --cask spark

brew install apache-spark
pip uninstall pyspark
pip install pyspark
```
### Add to ~/.zshrc
export SPARK_HOME=$(brew --prefix apache-spark)/libexec
export PATH="$SPARK_HOME/bin:$PATH"
export PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"