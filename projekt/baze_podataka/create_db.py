import psycopg2
from psycopg2.extras import execute_batch
import csv

try:
    conn = psycopg2.connect(
        database="transactions",
        user="postgres",
        port=5432,
        password="Rea123Teo",
        host="postgres_transactions"
    )
    conn.autocommit = False  # Ensure autocommit is disabled
except psycopg2.Error as e:
    print("Unable to connect to the database:", e)
    exit(1)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        ID_product VARCHAR(255) PRIMARY KEY,
        Product_name VARCHAR(255),
        UnitPrice FLOAT
    );
""")

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

# Preload existing products from the database
cur.execute("SELECT ID_product FROM Products")
existing_products = {row[0] for row in cur.fetchall()}

products_to_insert = []
transactions_to_insert = []

# Read data from CSV file and prepare for batch insertion
with open('csv/retail_all.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        stock_code = row['StockCode'].upper()
        if stock_code not in existing_products:
            existing_products.add(stock_code)
            products_to_insert.append((stock_code, row['Description'], float(row['UnitPrice'])))
        
        transactions_to_insert.append((stock_code, row['InvoiceNo'], int(row['Quantity']), row['InvoiceDate']))

# Batch insert into Products table
if products_to_insert:
    insert_products_query = """
        INSERT INTO Products (ID_product, Product_name, UnitPrice)
        VALUES (%s, %s, %s)
        ON CONFLICT (ID_product) DO NOTHING;
    """  # Added ON CONFLICT to handle duplicates gracefully
    execute_batch(cur, insert_products_query, products_to_insert, page_size=1000)

# Batch insert into Transactions table
if transactions_to_insert:
    insert_transactions_query = """
        INSERT INTO Transactions (ID_product, Invoice_number, Quantity, InvoiceDate)
        VALUES (%s, %s, %s, %s);
    """
    execute_batch(cur, insert_transactions_query, transactions_to_insert, page_size=1000)

conn.commit()
cur.close()
conn.close()

print("DONE")
