import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Set a seed for reproducibility
np.random.seed(42)

# Define the number of rows
num_rows = 100

# Define the number of stores and items
num_stores = 5
num_items = 30

# Generate store IDs (format: S001, S002, etc.)
store_ids = [f"S{str(i).zfill(3)}" for i in range(1, num_stores + 1)]

# Generate item numbers (format: ITM10001, ITM10002, etc.)
item_numbers = [f"ITM{10000 + i}" for i in range(1, num_items + 1)]

# Create categories and subcategories
categories = ["Electronics", "Clothing", "Home Goods", "Groceries", "Sports"]
subcategories = {
    "Electronics": ["Phones", "Laptops", "Accessories", "Audio"],
    "Clothing": ["Men's", "Women's", "Children's", "Footwear"],
    "Home Goods": ["Kitchen", "Bathroom", "Bedroom", "Decor"],
    "Groceries": ["Fresh", "Frozen", "Canned", "Snacks"],
    "Sports": ["Equipment", "Apparel", "Footwear", "Accessories"]
}

# Generate random data
data = []
for _ in range(num_rows):
    store_id = random.choice(store_ids)
    item_number = random.choice(item_numbers)

    # Ensure we don't have duplicate store_id and item_number combinations
    while any(row['store_id'] == store_id and row['item_number'] == item_number for row in data):
        store_id = random.choice(store_ids)
        item_number = random.choice(item_numbers)

    category = random.choice(categories)
    subcategory = random.choice(subcategories[category])

    # Generate a random date within the last 30 days for last_updated
    last_updated = datetime.now() - timedelta(days=random.randint(0, 30))

    row = {
        'store_id': store_id,
        'item_number': item_number,
        'category': category,
        'subcategory': subcategory,
        'quantity_on_hand': np.random.randint(0, 200),
        'reorder_point': np.random.randint(10, 50),
        'reorder_quantity': np.random.randint(20, 100),
        'unit_cost': round(np.random.uniform(5.0, 150.0), 2),
        'retail_price': 0,  # Will calculate below based on cost
        'last_received_date': (datetime.now() - timedelta(days=np.random.randint(1, 90))).strftime('%Y-%m-%d'),
        'last_sold_date': (datetime.now() - timedelta(days=np.random.randint(0, 30))).strftime('%Y-%m-%d'),
        'is_active': random.choice([True, True, True, False]),  # 75% chance of being active
        'supplier_id': f"SUP{np.random.randint(1000, 2000)}",
        'last_updated': last_updated.strftime('%Y-%m-%d %H:%M:%S')
    }

    # Calculate retail price (cost + random markup between 20% and 50%)
    markup = np.random.uniform(1.2, 1.5)
    row['retail_price'] = round(row['unit_cost'] * markup, 2)

    data.append(row)

# Convert to DataFrame
df = pd.DataFrame(data)

# Print the first 5 rows
print(df.head())

# Print summary statistics
print("\nSummary Statistics:")
print(f"Total number of rows: {len(df)}")
print(f"Number of unique stores: {df['store_id'].nunique()}")
print(f"Number of unique items: {df['item_number'].nunique()}")
print(f"Average quantity on hand: {df['quantity_on_hand'].mean():.2f}")
print(f"Average retail price: ${df['retail_price'].mean():.2f}")

# Export to CSV
df.to_csv("sample_inventory_data.csv", index=False)
print("\nData exported to 'sample_inventory_data.csv'")

# Also return the DataFrame as a dictionary to directly use the data
inventory_data = df.to_dict('records')
