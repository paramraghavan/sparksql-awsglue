# how to make problem 6 configuration look like using yaml

```shell
#export SPARK_HOME="/opt/homebrew/Cellar/apache-spark/3.3.2/libexec"
#export PATH="$SPARK_HOME/bin:$PATH"
#export PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"
```


```shell
pip install pyyaml
```

```python
with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)
```