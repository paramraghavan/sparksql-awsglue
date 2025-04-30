# Feature Engineering Pipeline
# Purpose: Create features for churn prediction model
"""
This notebook is structured and focused on a specific stage in the workflow. It imports functions from proper modules,
 has a clear purpose, and is designed to be part of a larger pipeline. The code is well-structured for eventual
  conversion to a production script.
"""

import os
import sys
sys.path.append("../../")  # Add project root to path

from src.utils.spark_utils import create_spark_session
from src.data.preprocessing import clean_customer_data
from src.features.build_features import create_engagement_features

# Initialize Spark with proper configuration
spark = create_spark_session("FeatureEngineering")

# Load cleaned data
df = spark.read.parquet("s3://my-bucket/data/cleaned/customers.parquet")

# Apply feature engineering functions from our modules
df_with_features = create_engagement_features(df)

# Save processed features for next pipeline stage
df_with_features.write.parquet("s3://my-bucket/data/features/customer_features.parquet")