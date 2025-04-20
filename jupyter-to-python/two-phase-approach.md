You're right - many data science teams primarily work with just two main phases for their notebooks. Let me simplify the
approach to better match your team's workflow:

## 1. Exploratory Notebooks (exploratory/)

**Example: `exploratory/customer_churn_analysis.ipynb`**

In the exploratory phase, data scientists:

- Investigate data properties
- Test hypotheses and correlations
- Create visualizations to understand patterns
- Try different algorithms and approaches
- Work in a non-linear, experimental way

```python
# Experimental analysis of customer transaction patterns
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load sample data
df = pd.read_csv("../../data/raw/transactions.csv")

# Quick stats and visualizations
print(df.describe())

# Look for patterns
plt.figure(figsize=(10, 6))
plt.scatter(df['purchase_amount'], df['days_since_last_visit'])
plt.title("Purchase Amount vs. Recency")
plt.xlabel("Purchase Amount ($)")
plt.ylabel("Days Since Last Visit")
plt.show()

# Test a quick model
from sklearn.cluster import KMeans

features = df[['purchase_amount', 'days_since_last_visit']].values
kmeans = KMeans(n_clusters=3).fit(features)
df['segment'] = kmeans.labels_

# Examine segment characteristics
df.groupby('segment').mean()
```

## 2. Production Notebooks (production/)

**Example: `production/customer_segmentation_pipeline.ipynb`**

In the production phase, notebooks:

- Follow a structured, linear workflow
- Import from proper Python modules in `src/`
- Include clear documentation
- Have well-defined inputs and outputs
- Are designed to be converted to scripts

```python
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
```

## Benefits of This Two-Phase Approach

1. **Clear transition path**:
    - Start in exploratory to develop understanding and approaches
    - Move to production when ready for standardization

2. **Proper separation of concerns**:
    - Exploratory: free-form, creative work
    - Production: structured, consistent patterns

3. **Easier workflow for data scientists**:
    - Familiar with just two phases to manage
    - Clear criteria for when to move from exploratory to production

4. **Simpler project organization**:
    - Two main notebook directories are easier to navigate
    - Clear distinction between what's experimental and what's production-ready

This simplified structure maintains the key benefits of organization while being more approachable for teams that prefer
a more streamlined workflow.

# Convert the Production Notebooks for submitting to emr

I'll help you convert your Jupyter notebook to a Spark submit script for EMR. Let me create a Python script version that
can be submitted to EMR via spark-submit.

Now I'll create a bash script to submit the Python script to EMR:

You'll also need to package your dependencies:

## Converting a Jupyter Notebook to a Spark-Submit Script for EMR

Here's what I've created to help you convert your Jupyter notebook to run on EMR:

1. **Python Script (customer_segmentation_pipeline.py)**: This is the main script converted from your notebook.
    - Removed interactive elements and notebook-specific code
    - Added proper entry point with `if __name__ == "__main__"`
    - Modified Spark session initialization to be compatible with spark-submit
    - Added error handling and logging
    - Updated paths to use S3 locations for both input and output

2. **Submit Script (submit_to_emr.sh)**: This script will:
    - Upload your Python script to S3
    - Submit the job to your EMR cluster with appropriate configurations
    - Set memory, executor, and core parameters

3. **Dependencies Packaging Script (package_dependencies.sh)**: This script:
    - Packages your project's source code into a zip file
    - Uploads it to S3 so it can be referenced in the spark-submit command

### Key Modifications Made:

1. **Spark Session Creation**: Modified to work with spark-submit rather than creating a new session from scratch
2. **Error Handling**: Added proper structuring with main() function
3. **Dependency Management**: Created a method to package and distribute your custom code
4. **S3 Integration**: Updated paths to use S3 for EMR compatibility
5. **Resource Configuration**: Added parameters for memory, cores, and executors

### Usage Instructions:

1. Run the dependencies packaging script first:
   ```
   chmod +x package_dependencies.sh
   ./package_dependencies.sh
   ```

2. Submit your job to EMR:
   ```
   chmod +x submit_to_emr.sh
   ./submit_to_emr.sh j-YOURCLUSTERID
   ```
