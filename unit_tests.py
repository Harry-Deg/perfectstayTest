import unittest
import sqlite3

class TestSQLQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = sqlite3.connect(':memory:')  # Utiliser une base de données en mémoire pour les tests
        cls.cur = cls.conn.cursor()
        cls.cur.execute('''
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            region TEXT NOT NULL
        )
        ''')        
        customers = [
            (1, 'Harry', 'West'),
            (2, 'Charlotte', 'East'),
            (3, 'Julien', 'North')
        ]
        cls.cur.executemany('''
        INSERT INTO customers (customer_id, customer_name, region)
        VALUES (?, ?, ?)
        ''', customers)
        print("Table 'customers' créée avec succès.")

        cls.cur.execute('''
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            purchase_price REAL
        )
        ''')
        donnees_products = [
            (1, 'Ordinateur', 'Electronique', 1200),
            (2, 'Livre', 'Livres', 45),
            (3, 'Chaise', 'Mobilier', 85),
        ]
        cls.cur.executemany('''
        INSERT INTO products (product_id, product_name, category, purchase_price)
        VALUES (?, ?, ?, ?)
        ''', donnees_products)
        print("Table 'products' créée avec succès.")

        cls.cur.execute('''
        CREATE TABLE IF NOT EXISTS "orders" (
            order_id INTEGER PRIMARY KEY,
            order_date TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            customer_id INTEGER NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (product_id),
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
        )
        ''')
        donnees_order = [
            (1, '2023-01-01', 1, 2, 1200.00, 1),
            (2, '2023-01-02', 2, 1, 45.00, 1),
            (3, '2023-01-03', 3, 1, 130.00, 2),
            (4, '2023-01-04', 1, 1, 1200.00, 3),
        ]
        cls.cur.executemany('''
        INSERT INTO "orders" (order_id, order_date, product_id, quantity, unit_price, customer_id)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', donnees_order)
        print("Table 'order' créée avec succès.")
        cls.conn.commit()


    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_sales(self):
        query =  """
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
        self.cur.execute(query)
        result = self.cur.fetchall()
        
        expected = [('Mobilier', 'East', '2023-01', 130.0, 45.0), ('Electronique', 'North', '2023-01', 1200.0, 0.0), ('Electronique', 'West', '2023-01', 2400.0, 0.0), ('Livres', 'West', '2023-01', 45.0, 0.0)]
        self.assertEqual(result, expected)

    def test_most_purchased_category(self):
        query = """
                WITH FavoriteCategory AS (
                    SELECT 
                        od.customer_id,
                        pd.category AS Favorite_Category,
                        SUM(od.quantity) AS SumQuantity
                    FROM 
                        Orders od
                    JOIN Products pd ON od.product_id = pd.product_id
                    GROUP BY 
                        od.customer_id,
                        pd.category
                    ORDER BY 
                        SumQuantity DESC

                )
                SELECT 
                    customer_id, 
                    Favorite_Category, 
                    max(SumQuantity) 
                FROM FavoriteCategory
                group by customer_id;
                """
        self.cur.execute(query)
        result = self.cur.fetchall()
        
        expected = [(1, 'Electronique', 2), (2, 'Mobilier', 1), (3, 'Electronique', 1)]
        self.assertEqual(result, expected)

    def test_most_purchased_product(self):
        query = """
                WITH FavoriteProduct AS (
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
                )
                SELECT 
                    customer_id, 
                    Favorite_Product, 
                    max(ProductQuantity) 
                FROM FavoriteProduct
                group by customer_id;
                """
        self.cur.execute(query)
        result = self.cur.fetchall()
        expected = [(1, 'Ordinateur', 2), (2, 'Chaise', 1), (3, 'Ordinateur', 1)]
        self.assertEqual(result, expected)
    
    def test_purchase_frequancy(self):
        query = """
                WITH PurchaseDiffs AS (
                  SELECT 
                    Customer_ID, 
                    Order_Date,
                    unit_price,
                    quantity,
                    JULIANDAY(Order_Date) - LAG(JULIANDAY(Order_Date)) OVER (PARTITION BY Customer_ID ORDER BY Order_Date) AS DaysBetweenOrders
                  FROM Orders
                ),
                AvgPurchaseFrequency AS (
                  SELECT 
                    Customer_ID,
                    SUM(quantity * unit_price) AS Total_Spent, 
                    AVG(DaysBetweenOrders) AS Purchase_Frequency
                  FROM PurchaseDiffs
                  WHERE DaysBetweenOrders IS NOT NULL
                  GROUP BY Customer_ID
                )
                SELECT 
                    Customer_ID,
                    Total_Spent, 
                    Purchase_Frequency 
                FROM AvgPurchaseFrequency;
                """
        self.cur.execute(query)
        result = self.cur.fetchall()
        expected = [(1, 45.0, 1.0)]
        self.assertEqual(result, expected)

    def test_customer_analysis(self):
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
        self.cur.execute(query)
        result = self.cur.fetchall()
        
        expected = [(1, 'West', 'Electronique', 45.0, 'Ordinateur', 1.0)]
        self.assertEqual(result, expected)
if __name__ == '__main__':
    unittest.main()




