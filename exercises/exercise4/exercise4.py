import pandas as pd

# Sample DataFrame
data = {
    'effective_date': ['2023-11-27 19:11:11', '2024-09-05 08:22:30', '2024-09-07 15:30:45'],
    'other_column': [10, 20, 30]
}
df = pd.DataFrame(data)

# Convert 'effective_date' column to datetime
df['effective_date'] = pd.to_datetime(df['effective_date'])

# Get today's date (midnight)
today_midnight = pd.Timestamp.today().normalize()

# Apply multiple filters: 'effective_date' <= today AND 'other_column' > 10
filtered_df = df[(df['effective_date'] <= today_midnight) & (df['other_column'] > 10)]

# Display the filtered DataFrame
filtered_df
