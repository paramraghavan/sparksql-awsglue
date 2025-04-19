I'll explain the purpose of different notebook folders with examples that demonstrate how they're used in a typical data
science workflow.

## 1. Exploratory Notebooks (exploratory/)

**Example: `exploratory/customer_churn_analysis.ipynb`**

```python
# Rough exploratory analysis of customer churn patterns
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Quick load of sample data
df = pd.read_csv("../../data/raw/customer_data_sample.csv")

# Basic statistics
print(df.describe())

# Look for correlations
corr = df.corr()
plt.figure(figsize=(12, 10))
sns.heatmap(corr, annot=True, cmap='coolwarm')
plt.title("Correlation between features")
plt.show()

# Try different segmentations
segments = df.groupby('subscription_type')['churn_rate'].mean()
print(segments)

# Quick test of a hypothesis
high_usage = df[df['monthly_usage'] > 20]['churn_rate'].mean()
low_usage = df[df['monthly_usage'] <= 20]['churn_rate'].mean()
print(f"High usage churn: {high_usage}, Low usage churn: {low_usage}")
```

This notebook is messy, experimental, and focused on discovery. It contains various visualizations and quick tests of
different hypotheses. The code is not organized for reuse.

## 2. Pipeline Notebooks (pipelines/)

**Example: `pipelines/feature_engineering.ipynb`**

```python
# Feature Engineering Pipeline
# Purpose: Create features for churn prediction model

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
```

This notebook is structured and focused on a specific stage in the workflow. It imports functions from proper modules,
has a clear purpose, and is designed to be part of a larger pipeline. The code is well-structured for eventual
conversion to a production script.

## 3. Reporting Notebooks (reporting/)

**Example: `reporting/churn_dashboard.ipynb`**

```python
# Churn Dashboard
# This notebook generates visualizations for the monthly churn report

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html

# Load production data
features = pd.read_parquet("s3://my-bucket/data/features/customer_features.parquet")
predictions = pd.read_parquet("s3://my-bucket/data/predictions/churn_predictions.parquet")

# Merge data
report_data = features.merge(predictions, on='customer_id')

# Create high-quality visualization for executives
fig = px.scatter(
    report_data,
    x="account_age",
    y="monthly_spend",
    color="predicted_churn_probability",
    size="total_purchases",
    hover_name="customer_segment",
    title="Customer Churn Risk by Account Age and Spending"
)

fig.update_layout(
    font=dict(family="Arial", size=14),
    legend_title="Churn Probability",
    coloraxis_colorbar=dict(title="Churn Risk")
)

# Save for inclusion in PowerPoint
fig.write_image("churn_risk_scatter.png", scale=2)

# Interactive dashboard elements
app = Dash(__name__)
app.layout = html.Div([
    html.H1("Customer Churn Dashboard"),
    dcc.Graph(figure=fig),
    # Additional dashboard components...
])
```

This notebook creates polished, presentation-ready visualizations. It focuses on communicating results rather than
analysis or pipeline processing. The output is designed for stakeholders to understand business implications.

## 4. Development Notebooks (development/)

**Example: `development/spark_partition_testing.ipynb`**

```python
# Testing Spark Partitioning Strategies
# Purpose: Find optimal partitioning for our datasets

import sys

sys.path.append("../../")

from src.utils.spark_utils import create_spark_session, time_execution
from src.utils.aws_utils import get_s3_file_size

# Create testing session with custom logging
spark = create_spark_session(
    "PartitionTest",
    {"spark.sql.shuffle.partitions": "200"}
)

# Load large test dataset
df = spark.read.parquet("s3://my-bucket/data/raw/large_dataset.parquet")
print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# Test different partitioning strategies and measure performance
results = []

for num_partitions in [50, 100, 200, 500]:
    # Repartition data
    df_test = df.repartition(num_partitions)

    # Run test query with timing
    time_taken = time_execution(lambda: df_test.groupBy("customer_id").count().collect())

    results.append({
        "partitions": num_partitions,
        "time_seconds": time_taken
    })

# Print results
for result in results:
    print(f"Partitions: {result['partitions']}, Time: {result['time_seconds']:.2f}s")

# Create recommended configuration
optimal = min(results, key=lambda x: x['time_seconds'])
print(f"Recommended spark.sql.shuffle.partitions: {optimal['partitions']}")
```

This notebook focuses on testing infrastructure configurations rather than data analysis. It helps optimize the
technical environment and would inform how production code is configured. The findings might be implemented in
configuration files or deployment scripts.

## Why This Organization Works

1. **Clear separation of concerns**:
    - Exploration stays experimental
    - Pipelines become production code
    - Reporting focuses on communication
    - Development tackles infrastructure issues

2. **Supports different phases of work**:
    - Data scientists can freely explore in exploratory notebooks
    - Engineers can focus on pipeline notebooks for productionization
    - Analysts can use reporting notebooks for business insights
    - DevOps can leverage development notebooks for infrastructure

3. **Facilitates proper code reuse**:
    - Core logic from pipelines moves to `src` modules
    - Notebooks become thinner orchestration layers
    - Infrastructure code developed in development notebooks moves to utilities

4. **Streamlined conversion to production**:
    - Pipeline notebooks convert directly to PySpark scripts
    - Infrastructure configurations tested in development notebooks become part of deployment
    - Exploratory findings inform but don't directly convert to production

This structure accommodates both the interactive, experimental nature of data science work and the need for
production-grade code that can run on a Spark cluster.