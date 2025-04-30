# Customer Segmentation Production Pipeline
# Purpose: Segment customers based on transaction history
# Author: Data Science Team
# Date: 2023-01-15

import os
import sys
sys.path.append("../../")  # Add project root to path

# Project imports
from src.utils.spark_utils import create_spark_session
from src.data.preprocessing import clean_transaction_data
from src.features.build_features import create_rfm_features
from src.models.clustering import train_kmeans_model, apply_clustering

# Initialize Spark
spark = create_spark_session("CustomerSegmentation")

# Load data
input_path = "s3://my-bucket/data/raw/transactions.parquet"
df = spark.read.parquet(input_path)
print(f"Loaded {df.count()} transactions")

# Preprocess data using project modules
df_clean = clean_transaction_data(df)

# Create RFM features
df_features = create_rfm_features(df_clean)

# Train segmentation model
model = train_kmeans_model(df_features, num_clusters=5, seed=42)

# Apply model
df_segmented = apply_clustering(df_features, model)

# Save results
output_path = "s3://my-bucket/data/processed/customer_segments.parquet"
df_segmented.write.parquet(output_path)
print(f"Saved customer segments to {output_path}")

# Also save model for future use
from src.utils.model_io import save_model
save_model(model, "models/customer_segmentation_model")