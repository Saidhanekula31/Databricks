# E-Commerce Transactions Analysis (PySpark)

This project analyzes e-commerce transactions using PySpark.  
It performs data cleaning, calculates revenues, finds top customers, summarizes sales by region, and more.

## Data Used
- **ecommerce_transactions.csv** – Contains order details such as `customer_id`, `unit_price`, `quantity`, `country`, `category`, and `month`.
- **country_region.csv** – Maps each country to its region.

## Steps Performed
1. **Load Data** from CSV files in DBFS.
2. **Clean Data** – Remove duplicates, filter missing customer IDs, and keep only positive quantities.
3. **Add Order Value** – `order_value = unit_price * quantity`.
4. **Country Revenue** – Total revenue for each country.
5. **Top Customers** – Identify the highest spender per country.
6. **Region Sales** – Join with region data and sum sales.
7. **Monthly Category Pivot** – Show monthly sales per category.
8. **Price Band Counts** – Classify transactions into price bands (`<50`, `50-100`, `100-200`, `200+`).
