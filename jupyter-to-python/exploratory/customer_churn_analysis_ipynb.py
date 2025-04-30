"""
This notebook is messy, experimental, and focused on discovery. It contains various visualizations and quick tests of different hypotheses. The code is not organized for reuse.
"""

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
