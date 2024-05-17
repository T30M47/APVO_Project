from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofweek, dayofmonth
from pyspark.sql.window import Window

# Inicijalizacija Spark sesije
spark = SparkSession.builder \
    .appName("Spark") \
    .getOrCreate()

# Korak 1: Kopiranje tablice Products iz baze transactions u bazu warehouse
products_transactions = spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres_transactions/transactions",
    driver="org.postgresql.Driver",
    dbtable="Products",
    user="postgres",
    password="Rea123Teo"
).load()

conn = psycopg2.connect(
    database="warehouse",
    user="postgres",
    port=5432,
    password="Rea123Teo",
    host="postgres_warehouse"
)

cur = conn.cursor()

drop_table_sql = f"DROP TABLE IF EXISTS Products;"
cur.execute(drop_table_sql)
conn.commit()

cur.execute("""
    CREATE TABLE Products (
        ID_product VARCHAR(255) PRIMARY KEY,
        Product_name VARCHAR(255),
        UnitPrice FLOAT,
        CONSTRAINT unique_ID_product UNIQUE (ID_product)
    );
""")

conn.commit()

cur.close()
conn.close()

# Spremanje tablice u bazu warehouse
products_transactions.write.format("jdbc").options(
    url="jdbc:postgresql://postgres_warehouse/warehouse",
    driver="org.postgresql.Driver",
    dbtable="Products",
    user="postgres",
    password="Rea123Teo"
).save(mode="append")

# Čitanje tablice Transactions iz baze transactions
transactions = spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres_transactions/transactions",
    driver="org.postgresql.Driver",
    dbtable="Transactions",
    user="postgres",
    password="Rea123Teo"
).load()

# Spajanje tablica i izračunavanje Amount
facts = transactions.join(products_transactions, transactions["ID_product"] == products_transactions["ID_product"]) \
    .select(
        transactions["ID_transaction"],
        transactions["ID_product"],
        transactions["Invoice_number"],
        F.round(col("UnitPrice") * col("Quantity"), 2).alias("Amount"),
        col("Quantity"),
        transactions["InvoiceDate"]
    )

conn = psycopg2.connect(
    database="warehouse",
    user="postgres",
    port=5432,
    password="Rea123Teo",
    host="postgres_warehouse"
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Transactions (
        ID_transaction SERIAL PRIMARY KEY,
        ID_product VARCHAR(255) REFERENCES Products(ID_product),
        Invoice_number VARCHAR(255),
        Amount FLOAT,
        Quantity INTEGER,
        InvoiceDate DATE
    );
""")

conn.commit()

cur.close()
conn.close()

facts.write.format("jdbc").options(
    url="jdbc:postgresql://postgres_warehouse/warehouse",
    driver="org.postgresql.Driver",
    dbtable="Transactions",
    user="postgres",
    password="Rea123Teo"
).save(mode="append")


df_deduplicated = transactions.dropDuplicates(["InvoiceDate"])

df_transformed = df_deduplicated.withColumn("Year", year("InvoiceDate")) \
    .withColumn("Month", month("InvoiceDate")) \
    .withColumn("Day", dayofmonth("InvoiceDate"))  \
    .withColumn("DayOfWeek", dayofweek("InvoiceDate") - 1)

window_spec = Window.orderBy("InvoiceDate")
df_transformed = df_transformed.withColumn("ID_time", F.row_number().over(window_spec))

# Step 6: Select relevant columns, including the new columns for year, month, day, day of week, and id_vrijeme
df_export = df_transformed.select("ID_time", "Year", "Month", "Day", "DayOfWeek")

conn = psycopg2.connect(
    database="warehouse",
    user="postgres",
    port=5432,
    password="Rea123Teo",
    host="postgres_warehouse"
)

cur = conn.cursor()

drop_table_sql_vrijeme= f"DROP TABLE IF EXISTS Time;"
cur.execute(drop_table_sql_vrijeme)
conn.commit()

cur.execute("""
    CREATE TABLE Time (
        ID_time INTEGER PRIMARY KEY,
        Year INTEGER,
        Month INTEGER,
        Day INTEGER,
        DayOfWeek INTEGER,
        CONSTRAINT unique_ID_time UNIQUE (ID_time)
    );
""")

conn.commit()

cur.close()
conn.close()

df_export.write.format("jdbc").options(
    url="jdbc:postgresql://postgres_warehouse/warehouse",
    driver="org.postgresql.Driver",
    dbtable="Time",
    user="postgres",
    password="Rea123Teo"
).save(mode="append")


transactions_warehouse = spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres_warehouse/warehouse",
    driver="org.postgresql.Driver",
    dbtable="transactions",
    user="postgres",
    password="Rea123Teo"
).load()

time = spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres_warehouse/warehouse",
    driver="org.postgresql.Driver",
    dbtable="time",
    user="postgres",
    password="Rea123Teo"
).load()

df_transformed = transactions_warehouse.join(
    time,
    (year(transactions_warehouse["invoicedate"]) == time["year"]) &
    (month(transactions_warehouse["invoicedate"]) == time["month"]) &
    (dayofmonth(transactions_warehouse["invoicedate"]) == time["day"]),
    "left_outer"
)

df_transformed_select = df_transformed.select(
    "id_transaction", "id_product", "id_time", "invoice_number", "amount", "quantity"
)

# Define the schema
schema = StructType([
    StructField("id_transaction", IntegerType(), nullable=False),
    StructField("id_product", StringType(), nullable=True),
    StructField("id_time", IntegerType(), nullable=True),
    StructField("invoice_number", StringType(), nullable=True),
    StructField("amount", FloatType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True)
])

# Enforce the schema
df_transformed_select = spark.createDataFrame(df_transformed_select.rdd, schema)

df_transformed_select.write.format("jdbc").options(
    url="jdbc:postgresql://postgres_warehouse/warehouse",
    driver="org.postgresql.Driver",
    dbtable="transactions",
    user="postgres",
    password="Rea123Teo"
).mode("overwrite").save()

spark.stop()