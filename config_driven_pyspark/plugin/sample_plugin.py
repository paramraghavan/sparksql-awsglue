from abc import ABC, abstractmethod
from pyspark import SparkContext
from pyspark.sql import DataFrame

def log_decorator(func):
    def wrapper(*args, **kwargs):
        print(f"Executing: {func.__name__}")
        result = func(*args, **kwargs)
        print(f"Finished: {func.__name__}")
        return result
    return wrapper

class SparkPluginInterface(ABC):
    def __init__(self, spark_context, app_context: dict = None):
        self.spark_context = spark_context
        if app_context == None:
            self.set_context()
        else:
            self.app_context = app_context

    @abstractmethod
    @log_decorator
    def set_context(self) -> None:
        """
        this is for the plugin to communicate with the platform.
        This way the plugin is self-contained
        we can set this up in the init as well and these values are read from
        config associate with the plug-in
        """
        pass

    @abstractmethod
    @log_decorator
    def extract_data(self) -> DataFrame:
        """
        Abstract method to read data.
        """
        pass


    @abstractmethod
    @log_decorator
    def transform_data(self, data: DataFrame) -> DataFrame:
        """
        Abstract method to apply transformation rules.
        """
        pass

    @abstractmethod
    @log_decorator
    def write_data(self, data: DataFrame) -> None:
        """
        Abstract method to write data in parquet format.
        """
        pass

class MySparkPlugin(SparkPluginInterface):

    def set_context(self) -> None:
        self.app_context = {}
        self.app_context['source_path'] = 'path_to_input.json'
        self.app_context['target_path'] = 'path_to_output'
        self.app_context['filter'] = ["column1", "column2"]

    def extract_data(self):
        # Example read logic
        df = self.spark_context.read.json(self.appContext['source_path'])
        return df


    def transform_data(self, data):
        # Example transformation logic
        #transformed_data = data.select("column1", "column2")
        transformed_data = data.select(self.app_context['filter'])
        return transformed_data

    def write_data(self, data):
        # Example logic to write data in parquet format
        data.write.parquet(self.app_context['target_path'])


# Usage example
if __name__ == "__main__":
    sc = SparkContext(appName="MySparkApp")
    plugin = MySparkPlugin(sc)
    data = plugin.extract_data()
    transformed_data = plugin.transform_data(data)
    plugin.write_data(transformed_data)