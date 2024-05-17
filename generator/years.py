import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('csv/retail_cleaned.csv')

# Convert InvoiceDate column to datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Replace years
df['InvoiceDate'] = df['InvoiceDate'].apply(lambda x: x.replace(year=2020))

# Save the modified DataFrame to a new CSV file
df.to_csv('csv/retail_2020.csv', index=False)

# Read the CSV file into a DataFrame
df = pd.read_csv('csv/prva_godina_45.csv')

# Convert InvoiceDate column to datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Replace years
df['InvoiceDate'] = df['InvoiceDate'].apply(lambda x: x.replace(year=2021))

# Save the modified DataFrame to a new CSV file
df.to_csv('csv/retail_2021.csv', index=False)

# Read the CSV file into a DataFrame
df = pd.read_csv('csv/druga_godina.csv')

# Convert InvoiceDate column to datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Replace years
df['InvoiceDate'] = df['InvoiceDate'].apply(lambda x: x.replace(year=2022))

# Save the modified DataFrame to a new CSV file
df.to_csv('csv/retail_2022.csv', index=False)

# Read the CSV file into a DataFrame
df = pd.read_csv('csv/treća_godina_65.csv')

# Convert InvoiceDate column to datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Replace years
df['InvoiceDate'] = df['InvoiceDate'].apply(lambda x: x.replace(year=2023))

# Save the modified DataFrame to a new CSV file
df.to_csv('csv/retail_2023.csv', index=False)
