import pandas as pd
import matplotlib.pyplot as plt

# Load the data into a DataFrame
data = pd.read_csv('csv/druga_godina.csv')
#data = pd.read_csv('generator/csv/retail_cleaned.csv')

# Convert 'InvoiceDate' column to datetime format
data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])

# Extract month from 'InvoiceDate'
data['Month'] = data['InvoiceDate'].dt.month

# Group by month and count the number of transactions
monthly_transactions = data.groupby('Month').size()

# Plotting
plt.figure(figsize=(10, 6))
monthly_transactions.plot(kind='bar', color='skyblue')
plt.title('Number of Transactions per Month')
plt.xlabel('Month')
plt.ylabel('Number of Transactions')
plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.tight_layout()
plt.show()
