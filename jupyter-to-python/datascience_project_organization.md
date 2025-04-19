# Data Science Project Organization for Jupyter to PySpark Conversion

## Jupyter Notebook Integration

This organization structure emphasizes a smooth workflow where data scientists can work with Jupyter notebooks for
exploratory analysis and development while ensuring code can be properly modularized and converted to production PySpark
jobs:

1. **Development starts in notebooks**: Data scientists begin work in Jupyter notebooks
2. **Reusable code moves to modules**: As code matures, core functions move to the `src` directory
3. **Notebooks import from modules**: Notebooks then import functions from proper Python modules
4. **Scripts are created for production**: Final production code is converted to standalone scripts

## Project Structure

```
project-root/
├── README.md               # Project documentation
├── .gitignore              # Git ignore file
├── requirements.txt        # Python dependencies
├── setup.py                # Package installation
├── config/                 # Configuration files
│   ├── spark_config.py     # Spark configuration
│   └── aws_config.py       # AWS configuration
├── data/                   # Data directory (or references to S3)
│   ├── raw/                # Raw input data
│   ├── processed/          # Processed data
│   └── output/             # Output data
├── notebooks/              # Jupyter notebooks (development only)
│   ├── exploratory/        # Exploratory analysis notebooks
│   │   ├── data_exploration.ipynb
│   │   ├── feature_analysis.ipynb
│   │   └── model_experimentation.ipynb
│   ├── pipelines/          # Data processing pipelines
│   │   ├── data_ingestion.ipynb
│   │   ├── feature_engineering.ipynb
│   │   ├── model_training.ipynb
│   │   └── model_evaluation.ipynb
│   ├── reporting/          # Result visualization notebooks
│   │   ├── performance_dashboard.ipynb
│   │   └── business_insights.ipynb
│   └── development/        # Development notebooks
│       ├── spark_job_dev.ipynb
│       ├── aws_integration.ipynb
│       └── prototype_algorithms.ipynb
├── src/                    # Source code (importable packages)
│   ├── __init__.py
│   ├── data/               # Data processing modules
│   │   ├── __init__.py
│   │   ├── ingestion.py    # Data ingestion functions
│   │   └── preprocessing.py # Data preprocessing functions
│   ├── features/           # Feature engineering
│   │   ├── __init__.py
│   │   └── build_features.py # Feature creation
│   ├── models/             # Model training and evaluation
│   │   ├── __init__.py
│   │   ├── train.py        # Model training
│   │   └── evaluate.py     # Model evaluation
│   └── utils/              # Utilities
│       ├── __init__.py
│       ├── spark_utils.py  # PySpark utility functions
│       └── aws_utils.py    # AWS utility functions
├── scripts/                # Runnable scripts for production
│   ├── train_model.py      # Script to train model
│   └── predict.py          # Script to make predictions
└── tests/                  # Unit tests
    ├── __init__.py
    ├── test_data.py
    └── test_models.py
```

## Best Practices

### 1. Modularize Code into Python Packages

Instead of writing all code in notebooks, implement reusable functions and classes in proper Python modules:

```python
# src/data/ingestion.py
def read_from_s3(bucket, key, spark):
    """
    Read data from S3 using PySpark
    
    Parameters:
    -----------
    bucket : str
        S3 bucket name
    key : str
        S3 object key
    spark : SparkSession
        Active Spark session
        
    Returns:
    --------
    DataFrame
        PySpark DataFrame with loaded data
    """
    path = f"s3a://{bucket}/{key}"
    return spark.read.parquet(path)
```

### 2. Import Custom Modules in Notebooks

In notebooks, import modules from your project structure:

```python
# Import from your project modules
import sys

sys.path.append('..')  # Add parent directory to path

from src.data.ingestion import read_from_s3
from src.utils.spark_utils import create_spark_session

# Now use these functions
spark = create_spark_session("MyApp")
df = read_from_s3("my-bucket", "path/to/data.parquet", spark)
```

### 3. Configuration Management

Store configuration separately:

```python
# config/spark_config.py
def get_spark_config():
    """Return spark configuration for the application"""
    return {
        "spark.executor.memory": "16g",
        "spark.driver.memory": "8g",
        "spark.executor.cores": "4",
        "spark.driver.cores": "2",
        "spark.dynamicAllocation.enabled": "true",
        "spark.shuffle.service.enabled": "true"
    }
```

### 4. Handle External Java Dependencies

For Java libraries, use proper dependency management:

```python
# setup.py
from setuptools import setup, find_packages

setup(
    name="myproject",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.1.0",
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        # Other dependencies
    ],
    # Java/Scala dependencies can be managed with package_data
    package_data={
        "myproject": ["jars/*.jar"]
    }
)
```

Then in your code:

```python
# src/utils/spark_utils.py
import os


def create_spark_session(app_name, jars_dir=None):
    """
    Create a SparkSession with custom jars
    
    Parameters:
    -----------
    app_name : str
        Name of the Spark application
    jars_dir : str, optional
        Directory containing JAR files
        
    Returns:
    --------
    SparkSession
        Configured Spark session
    """
    from pyspark.sql import SparkSession

    # Default path for jars is inside the package
    if jars_dir is None:
        import myproject
        package_dir = os.path.dirname(myproject.__file__)
        jars_dir = os.path.join(package_dir, "jars")

    # Get all jar files
    if os.path.exists(jars_dir):
        jars = [os.path.join(jars_dir, f) for f in os.listdir(jars_dir) if f.endswith('.jar')]
        jars_str = ",".join(jars)
    else:
        jars_str = ""

    # Create session with jars
    builder = SparkSession.builder.appName(app_name)

    if jars_str:
        builder = builder.config("spark.jars", jars_str)

    # Add other configurations
    from myproject.config.spark_config import get_spark_config
    for key, value in get_spark_config().items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
```

### 5. Replace Shell Commands with Python Code

Instead of using shell commands with `!`, use Python equivalents:

```python
# Instead of !aws s3 cp
import boto3


def copy_to_s3(local_path, bucket, key):
    """Copy a local file to S3"""
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_path, bucket, key)


# Instead of !cd
import os


def process_in_directory(dir_path, func, *args, **kwargs):
    """Execute a function in a specific directory"""
    current_dir = os.getcwd()
    try:
        os.chdir(dir_path)
        return func(*args, **kwargs)
    finally:
        os.chdir(current_dir)
```

### 6. Making Notebooks Convertible to Scripts

Use a standard structure in notebooks to make conversion easier:

```python
# %% [markdown]
# # Data Processing Notebook
# This notebook handles data processing for our project

# %% [markdown]
# ## Setup and Imports

# %%
# Standard imports
import pandas as pd
import numpy as np

# Project imports
import sys

sys.path.append("../../")  # Add project root to path to import from src
from src.utils.spark_utils import create_spark_session
from src.data.ingestion import read_from_s3

# Initialize Spark
spark = create_spark_session("DataProcessing")

# %% [markdown]
# ## Data Loading

# %%
# Load data
df = read_from_s3("my-bucket", "path/to/data.parquet", spark)
df.printSchema()

# %% [markdown]
# ## Data Processing

# %%
# Processing code
# ...

# %% [markdown]
# ## Save Results

# %%
# Save results
from src.data.ingestion import write_to_s3

write_to_s3(processed_df, "output-bucket", "path/to/output.parquet")
```

### 7. Notebook to Production Script Conversion Process

Here's a systematic approach to converting notebooks to production scripts:

1. **Develop in notebooks first**:
    - Use notebooks for exploration and rapid iteration
    - Break code into logical cells with markdown documentation
    - Keep notebooks in version control

2. **Extract reusable functions**:
    - Move core functionality to Python modules in the `src` directory
    - Create proper classes and functions with docstrings
    - Leave notebooks with only workflow orchestration and visualization

3. **Convert using automated tools**:
    - Use `nbconvert` to convert notebooks to Python scripts
   ```bash
   jupyter nbconvert --to python notebooks/pipelines/model_training.ipynb
   ```
    - Use `papermill` for parameterized notebook execution
   ```bash
   papermill notebooks/pipelines/data_ingestion.ipynb /tmp/output.ipynb -p date "2023-01-01" -p input_path "s3://bucket/path"
   ```

4. **Refactor scripts for production**:
    - Add proper command-line argument handling
    - Add error handling and logging
    - Remove visualization code
    - Use environment variables for configuration

### 7. Data and File Path Management

Use relative paths and environment variables:

```python
# src/utils/path_utils.py
import os


def get_project_root():
    """Get the absolute path to the project root directory"""
    # This assumes utils is always at src/utils in the project structure
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(current_dir, '..', '..'))


def get_data_path(data_type="raw"):
    """Get path to a data directory"""
    valid_types = ["raw", "processed", "output"]
    if data_type not in valid_types:
        raise ValueError(f"data_type must be one of {valid_types}")

    # Check environment variable first
    env_var = f"PROJECT_DATA_{data_type.upper()}"
    if env_var in os.environ:
        return os.environ[env_var]

    # Otherwise, use local path
    root = get_project_root()
    return os.path.join(root, "data", data_type)
```

### 8. Example Jupyter Notebook

Here's a typical well-structured Jupyter notebook (`notebooks/pipelines/feature_engineering.ipynb`) that follows best
practices:

```python
# %% [markdown]
# # Feature Engineering Pipeline
# 
# This notebook processes raw data and creates features for model training.
# 
# Author: Data Scientist
# Date: 2023-01-15
# 
# ## Purpose
# - Load raw customer transaction data
# - Create time-based features
# - Apply feature scaling
# - Save processed features for model training

# %% [markdown]
# ## 1. Setup and Imports

# %%
# Standard library imports
import os
import sys
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

# Third-party imports
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler

# Project imports
from src.utils.spark_utils import create_spark_session
from src.data.ingestion import read_from_s3, write_to_s3
from src.features.build_features import create_time_features, create_customer_features

# %% [markdown]
# ## 2. Initialize Spark Session

# %%
# Set log level to reduce verbosity
import logging

logging.getLogger("py4j").setLevel(logging.ERROR)

# Create spark session with appropriate configurations
spark = create_spark_session(
    app_name="FeatureEngineering",
    jars_dir="../../jars"  # Path to custom JARs relative to notebook
)
print(f"Spark version: {spark.version}")

# %% [markdown]
# ## 3. Load Raw Data

# %%
# Configuration - in practice, these could come from a config file
INPUT_BUCKET = "my-data-lake"
INPUT_PATH = "raw/transactions/2023-01/"
OUTPUT_BUCKET = "my-data-lake"
OUTPUT_PATH = "features/customer_features_2023_01"

# Load data
raw_df = read_from_s3(INPUT_BUCKET, INPUT_PATH, spark)
print(f"Loaded {raw_df.count()} records")
raw_df.printSchema()

# Display sample
raw_df.limit(5).toPandas()

# %% [markdown]
# ## 4. Create Features

# %%
# Create time-based features
df_with_time = create_time_features(raw_df)
df_with_time.cache()  # Cache for performance

# Create customer-level features
customer_features = create_customer_features(df_with_time)

# Show feature distributions
customer_features.describe().show()

# %% [markdown]
# ## 5. Feature Scaling

# %%
# Select numeric columns for scaling
numeric_cols = [f.name for f in customer_features.schema.fields
                if isinstance(f.dataType, (DoubleType, IntegerType))]

# Assemble features into vector
assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features_raw")
assembled_df = assembler.transform(customer_features)

# Scale features
scaler = StandardScaler(inputCol="features_raw", outputCol="features_scaled")
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)

# %% [markdown]
# ## 6. Save Results

# %%
# Save processed features
write_to_s3(scaled_df, OUTPUT_BUCKET, OUTPUT_PATH)
print(f"Saved features to s3://{OUTPUT_BUCKET}/{OUTPUT_PATH}")

# Save scaler model for later use
from src.models.utils import save_ml_model

save_ml_model(scaler_model, "models/scaler_model")

# %% [markdown]
# ## 7. Cleanup

# %%
# Unpersist cached DataFrames
df_with_time.unpersist()

# Stop Spark session
spark.stop()
```

### 9. Script Conversion Example

Here's how a notebook might be converted to a script:

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data processing script converted from Jupyter notebook
"""

import argparse
import logging

from src.utils.spark_utils import create_spark_session
from src.data.ingestion import read_from_s3, write_to_s3
from src.data.preprocessing import clean_data, transform_data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_data(input_bucket, input_key, output_bucket, output_key):
    """Main data processing function"""
    logger.info("Initializing Spark session")
    spark = create_spark_session("DataProcessingJob")

    try:
        logger.info(f"Reading data from s3://{input_bucket}/{input_key}")
        df = read_from_s3(input_bucket, input_key, spark)

        logger.info("Cleaning data")
        df_cleaned = clean_data(df)

        logger.info("Transforming data")
        df_transformed = transform_data(df_cleaned)

        logger.info(f"Writing results to s3://{output_bucket}/{output_key}")
        write_to_s3(df_transformed, output_bucket, output_key)

        logger.info("Processing completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data using PySpark")
    parser.add_argument("--input-bucket", required=True, help="Input S3 bucket name")
    parser.add_argument("--input-key", required=True, help="Input S3 object key")
    parser.add_argument("--output-bucket", required=True, help="Output S3 bucket name")
    parser.add_argument("--output-key", required=True, help="Output S3 object key")

    args = parser.parse_args()
    exit_code = process_data(
        args.input_bucket,
        args.input_key,
        args.output_bucket,
        args.output_key
    )

    exit(exit_code)
```

## Testing

Include tests for your modules:

```python
# tests/test_data.py
import unittest
from unittest.mock import patch, MagicMock

from src.data.ingestion import read_from_s3


class TestDataIngestion(unittest.TestCase):

    @patch("pyspark.sql.SparkSession")
    def test_read_from_s3(self, mock_spark):
        # Setup mock
        mock_spark_instance = MagicMock()
        mock_reader = MagicMock()
        mock_spark_instance.read.return_value = mock_reader
        mock_reader.parquet.return_value = "dataframe"

        # Call function
        result = read_from_s3("test-bucket", "test-key", mock_spark_instance)

        # Assertions
        mock_reader.parquet.assert_called_once_with("s3a://test-bucket/test-key")
        self.assertEqual(result, "dataframe")


if __name__ == "__main__":
    unittest.main()
```

## Documentation

Document your code and project:

```python
"""
Project documentation using docstrings.
This follows Google style docstring format.

Example:
    Examples can be given using either the ``Example`` or ``Examples``
    sections. Sections support any reStructuredText formatting, including
    literal blocks::

        $ python example.py

Section breaks are created with two blank lines. Section breaks are also
implicitly created anytime a new section starts.
"""
```