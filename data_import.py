import pandas as pd
import sqlite3

dfCustomers = pd.read_csv('resources/customers.csv')
dfOrders = pd.read_csv('resources/orders.csv')
dfProducts = pd.read_csv('resources/products.csv')

conn = sqlite3.connect('Sales.db')

dfCustomers.to_sql('Customers', conn, if_exists='replace', index=False)
dfOrders.to_sql('Orders', conn, if_exists='replace', index=False)
dfProducts.to_sql('Products', conn, if_exists='replace', index=False)
