import pandas as pd

# Load the data into a DataFrame
data = pd.read_csv('projekt/csv/retail_cleaned.csv')

# Convert the 'InvoiceDate' column to datetime format
data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])

# Extract year and month from the 'InvoiceDate' column
data['YearMonth'] = data['InvoiceDate'].dt.to_period('M')

# Group by product and month and sum the quantities sold
product_sales_by_month = data.groupby(['Description', 'YearMonth'])['Quantity'].sum()

# Get the top sold product for each month
top_products_by_month = product_sales_by_month.groupby(level='YearMonth').idxmax()

# Print top sold product for every month
for month, product in top_products_by_month.iteritems():
    print(f"{month}: {product}")
