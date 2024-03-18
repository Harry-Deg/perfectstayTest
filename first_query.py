import pandas as pd
import sqlite3

conn = sqlite3.connect('Sales.db')
cursor = conn.cursor()

query = """
SELECT 
    pd.category as Category, 
    ct.region as Region, 
    strftime('%Y-%m', od.order_date) AS Month, 
    sum(od.quantity * od.unit_price) as Total_Sales,  
    SUM((od.quantity * od.unit_price) - (od.quantity * pd.purchase_price)) AS Margin
FROM 
    orders od
JOIN 
    products pd ON od.product_id = pd.product_id
JOIN
    customers ct ON od.customer_id = ct.customer_id
GROUP BY 
    Month,
    Region,
    Category;
"""

df_sales = pd.read_sql_query(query, conn)

df_sales.to_sql('Sales_report', conn, if_exists='replace', index=False)
df_sales.to_csv('sales_report.csv', index=False)