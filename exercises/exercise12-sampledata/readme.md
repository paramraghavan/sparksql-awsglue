I'll create a sample inventory dataset with 100 rows containing store_id and item_number as key columns, along with
other relevant inventory fields.

Here's a sample CSV of the inventory data with 100 rows using store_id and item_number as key columns:

The sample inventory data includes 100 rows with the following characteristics:

### Key Columns:

- `store_id`: Unique store identifier (format: S001, S002, etc.)
- `item_number`: Unique item identifier (format: ITM10001, ITM10002, etc.)

### Additional Fields:

- `category`: Product category (Electronics, Clothing, Home Goods, Groceries, Sports)
- `subcategory`: Specific product subcategory
- `quantity_on_hand`: Current inventory level
- `reorder_point`: Inventory level that triggers reordering
- `reorder_quantity`: Quantity to order when restocking
- `unit_cost`: Cost per unit
- `retail_price`: Selling price (calculated as cost + markup)
- `last_received_date`: Date when inventory was last received
- `last_sold_date`: Date when the item was last sold
- `is_active`: Whether the item is currently being sold (75% true, 25% false)
- `supplier_id`: Identifier for the supplier
- `last_updated`: Timestamp of the last inventory update

The data includes a variety of stores (5 unique stores) and items (30 unique item numbers) with realistic inventory
quantities, pricing, and dates. T