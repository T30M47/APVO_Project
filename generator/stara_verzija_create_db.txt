import psycopg2
from psycopg2 import sql
import csv

try:
    conn = psycopg2.connect(
        database="transactions",
        user="postgres",
        port=5432,
        password="Rea123Teo",
        host="postgres_transactions"
    )
except psycopg2.Error as e:
    print("Unable to connect to the database:", e)
        
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        ID_product VARCHAR(255) PRIMARY KEY,
        Product_name VARCHAR(255),
        UnitPrice FLOAT
    );
""")

conn.commit()

# Create Transactions table
cur.execute("""
    CREATE TABLE IF NOT EXISTS Transactions (
        ID_transaction SERIAL PRIMARY KEY,
        ID_product VARCHAR(255) REFERENCES Products(ID_product),
        Invoice_number VARCHAR(255),
        Quantity INTEGER,
        InvoiceDate DATE
    );
""")

conn.commit()

print("Created Transactions!")

existing_products = {}

# Read data from CSV file and insert into PostgreSQL
with open('csv/retail_all.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        if row['StockCode'].upper() not in existing_products:
            # Insert the product into the Products table
            cur.execute("""
                INSERT INTO Products (ID_product, Product_name, UnitPrice)
                VALUES (%s, %s, %s);
            """, (row['StockCode'].upper(), row['Description'], float(row['UnitPrice'])))
            # Add the product to the dictionary
            existing_products[row['StockCode'].upper()] = True

        cur.execute("""
            INSERT INTO Transactions (ID_product, Invoice_number, Quantity, InvoiceDate)
            VALUES (%s, %s, %s, %s);
        """, (row['StockCode'].upper(), row['InvoiceNo'], row['Quantity'], row['InvoiceDate']))


conn.commit()
cur.close()
conn.close()

print("DONE")