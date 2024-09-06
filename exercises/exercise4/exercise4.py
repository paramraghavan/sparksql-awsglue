import pandas as pd
from datetime import datetime

# Example dataframe
data = {
    'effective_date': ['2023-11-27 19:11:11', '2024-09-05 08:22:30', '2028-09-07 15:30:45'],
    'other_column': [10, 20, 30]
}
df = pd.DataFrame(data)

# Convert the 'effective_date' column to datetime format
df['effective_date'] = pd.to_datetime(df['effective_date'])

# Get today's date
#today = pd.to_datetime('2024-09-06')
today = pd.Timestamp.now().normalize()

# Apply filter to get rows with 'effective_date' <= today's date
filtered_df = df[df['effective_date'] <= today]

# Display the filtered DataFrame
filtered_df
