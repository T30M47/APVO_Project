import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('csv/retail_2021_final.csv')

# Convert InvoiceDate column to datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m-%d')

# Add years
df['InvoiceDate'] = df['InvoiceDate'] + pd.DateOffset(years=2021 - df['InvoiceDate'].dt.year.min())

# Save the modified DataFrame to a new CSV file
df.to_csv('csv/retail_2021_years.csv', index=False)

# Read the CSV file into a DataFrame
df = pd.read_csv('csv/retail_2022_final.csv')

# Convert InvoiceDate column to datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m-%d')

# Add years
df['InvoiceDate'] = df['InvoiceDate'] + pd.DateOffset(years=2022 - df['InvoiceDate'].dt.year.min())

# Save the modified DataFrame to a new CSV file
df.to_csv('csv/retail_2022_years.csv', index=False)

# Read the CSV file into a DataFrame
df = pd.read_csv('csv/retail_2023_final.csv')

# Convert InvoiceDate column to datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m-%d')

# Add years
df['InvoiceDate'] = df['InvoiceDate'] + pd.DateOffset(years=2023 - df['InvoiceDate'].dt.year.min())

# Save the modified DataFrame to a new CSV file
df.to_csv('csv/retail_2023_years.csv', index=False)