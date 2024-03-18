import pandas as pd
import sqlite3

conn = sqlite3.connect('Sales.db')
cursor = conn.cursor()

query = """
WITH PurchaseDiffs AS (
  SELECT 
    customer_id, 
    quantity,
    unit_price,
    order_date,
    JULIANDAY(order_date) - LAG(JULIANDAY(order_date)) OVER (PARTITION BY customer_id ORDER BY order_date) AS DaysBetweenOrders
  FROM Orders
),
AvgPurchaseFrequency AS (
  SELECT 
    customer_id, 
    SUM(quantity * unit_price) AS Total_Spent,
    AVG(DaysBetweenOrders) AS Purchase_Frequency
  FROM PurchaseDiffs
  WHERE DaysBetweenOrders IS NOT NULL
  GROUP BY customer_id
),
FavoriteCategory AS (
    SELECT 
        od.customer_id,
        pd.category AS Favorite_Category,
        SUM(od.quantity) AS SumQuantity
    FROM 
        Orders od
    JOIN Products pd ON od.product_id = pd.product_id
    GROUP BY 
        od.Customer_id,
        pd.category
    ORDER BY 
        SumQuantity DESC
),
FavoriteCategoryCustomer AS (
    SELECT 
        customer_id, 
        Favorite_Category, 
        max(SumQuantity) 
    FROM FavoriteCategory
    GROUP BY customer_id
),
FavoriteProduct AS (
    SELECT 
        od.customer_id,
        pd.product_name AS Favorite_Product,
        SUM(od.quantity) AS ProductQuantity
    FROM 
        Orders od
    JOIN Products pd ON od.product_id = pd.product_id
    GROUP BY 
        od.customer_id,
        pd.product_name
    ORDER BY 
        ProductQuantity DESC
),
FavoriteProductCustomer AS (
    SELECT 
        customer_id, 
        Favorite_Product, 
        max(ProductQuantity) 
    FROM FavoriteProduct
    GROUP BY customer_id
)
SELECT 
    ct.customer_id,
    ct.region,
    fc.Favorite_Category,
    apf.Total_Spent,
    fp.Favorite_Product,
    apf.Purchase_Frequency
FROM 
    Customers ct
JOIN 
    AvgPurchaseFrequency apf ON ct.customer_id = apf.customer_id
JOIN 
    FavoriteCategoryCustomer fc ON ct.customer_id = fc.customer_id
JOIN 
    FavoriteProductCustomer fp ON ct.customer_id = fp.customer_id;
"""

df_customer_analysis = pd.read_sql_query(query, conn)

df_customer_analysis.to_sql('customer_analysis', conn, if_exists='replace', index=False)
df_customer_analysis.to_csv('customer_analysis.csv', index=False)