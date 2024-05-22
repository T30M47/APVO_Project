import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import psycopg2
import pandas as pd
from prophet import Prophet
import plotly.graph_objs as go

# Define the connection parameters for your PostgreSQL warehouse
db_params = {
    'host': 'postgres_warehouse',
    'database': 'warehouse',
    'user': 'postgres',
    'password': 'Rea123Teo',
}

conn = psycopg2.connect(**db_params)
# Query to fetch transactions data
transactions_query = """
SELECT tm.year || '-' || tm.month || '-' || tm.day AS date, SUM(t.amount) AS amount
FROM transactions t
JOIN time tm ON t.id_time = tm.id_time
GROUP BY date
ORDER BY date;
"""

# Query to fetch product sales data
product_sales_query = """
SELECT tm.year || '-' || tm.month || '-' || tm.day AS date, p.product_name, SUM(t.quantity) AS total_quantity
FROM transactions t
JOIN products p ON t.id_product = p.id_product
JOIN time tm ON t.id_time = tm.id_time
GROUP BY date, p.product_name
ORDER BY date, total_quantity DESC;
"""


# Execute the queries and fetch data into pandas dataframes
transactions_df = pd.read_sql_query(transactions_query, conn)
# product_sales_df = pd.read_sql_query(product_sales_query, conn)
transactions_df.rename(columns={'date': 'ds', 'amount': 'y'}, inplace=True)
# Fetch the top 5 most sold products
product_sales_df = pd.read_sql_query(product_sales_query, conn)
top_5_products = product_sales_df.groupby('product_name').sum().nlargest(5, 'total_quantity').index.tolist()

# Create a dictionary to hold dataframes for top 5 products
top_product_dataframes = {}

# Create dataframe for each top product
for product in top_5_products:
    product_df = product_sales_df[product_sales_df['product_name'] == product].copy()
    product_df.rename(columns={'date': 'ds', 'total_quantity': 'y'}, inplace=True)
    top_product_dataframes[product] = product_df[['ds', 'y']]

conn.close()

# daily_transactions_df = pd.read_sql_query(daily_transactions_query, conn)
# daily_transactions_df.rename(columns={'date': 'ds', 'transaction_count': 'y'}, inplace=True)
# # Close the database connection
# conn.close()
# # Concatenate product name and quantity into a single column
# product_sales_df['product_name'] = product_sales_df['product_name'] + ' - ' + product_sales_df['quantity'].astype(str)

# # Drop the original quantity column
# product_sales_df.drop(columns=['quantity'], inplace=True)

# # Rename the date column to 'date'
# product_sales_df.rename(columns={'date': 'date'}, inplace=True)
def make_prophet_forecast(df, days):
    m = Prophet()
    m.fit(df)
    future = m.make_future_dataframe(periods=days)
    forecast = m.predict(future)
    return forecast

# Make forecasts for top 5 products
forecast_top_product_dataframes = {}
for product, df in top_product_dataframes.items():
    forecast_top_product_dataframes[product] = make_prophet_forecast(df, 30)

# Save forecasts to CSV files
for product, forecast_df in forecast_top_product_dataframes.items():
    filename = f'forecast_{product.replace(" ", "_").lower()}.csv'
    forecast_df.to_csv(filename, index=False)


# Izračunajte predviđanja za 7 i 30 dana
# forecast_transactions_30_days = make_prophet_forecast(transactions_df, 30)
# forecast_transactions_183_days = make_prophet_forecast(transactions_df, 183)
# forecast_transactions_365_days = make_prophet_forecast(transactions_df, 365)
# # forecast_daily_transactions_7_days = make_prophet_forecast(daily_transactions_df, 7)
# # forecast_daily_transactions_30_days = make_prophet_forecast(daily_transactions_df, 30)

# # Pohranite predviđanja u CSV datoteke
# forecast_transactions_30_days.to_csv('forecast_transactions_30_days.csv', index=False)
# forecast_transactions_183_days.to_csv('forecast_transactions_183_days.csv', index=False)
# forecast_transactions_365_days.to_csv('forecast_transactions_365_days.csv', index=False)

# forecast_daily_transactions_7_days.to_csv('forecast_product_sales_7_days.csv', index=False)
# forecast_daily_transactions_30_days.to_csv('forecast_product_sales_30_days.csv', index=False)

# import psycopg2
# import pandas as pd
# from prophet import Prophet

# # Define the connection parameters for your PostgreSQL warehouse
# db_params = {
#     'host': 'postgres_warehouse',
#     'database': 'warehouse',
#     'user': 'postgres',
#     'password': 'Rea123Teo',
# }

# conn = psycopg2.connect(**db_params)
# # Query to fetch all transactions data
# transactions_query = """
# SELECT tm.year || '-' || tm.month || '-' || tm.day AS date, t.amount
# FROM transactions t
# JOIN time tm ON t.id_time = tm.id_time
# ORDER BY date;
# """

# # Execute the query and fetch data into pandas dataframe
# transactions_df = pd.read_sql_query(transactions_query, conn)
# transactions_df.rename(columns={'date': 'ds', 'amount': 'y'}, inplace=True)

# # Close the database connection
# conn.close()

# def make_prophet_forecast(df, days):
#     m = Prophet()
#     m.fit(df)
#     future = m.make_future_dataframe(periods=days)
#     forecast = m.predict(future)
#     return forecast

# # Calculate predictions for 30 days
# forecast_transactions_30_days = make_prophet_forecast(transactions_df, 30)

# # Save the predictions to a CSV file
# forecast_transactions_30_days.to_csv('forecast_transactions_30_days.csv', index=False)
