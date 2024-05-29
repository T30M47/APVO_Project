import pandas as pd

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('projekt/csv/retail_2021_final.csv')

# Check for null/nan values in the 'InvoiceNo' column
"""null_invoice_numbers = df[df['InvoiceNo'].isnull()]

# Count the number of rows with null/nan values in 'InvoiceNo' column
count_null_invoice_numbers = null_invoice_numbers.shape[0]

print("Number of rows with null/nan values in 'InvoiceNo' column:", count_null_invoice_numbers)"""

# Check for null/nan values in the 'InvoiceNo' column
null_invoice_numbers = df[df['InvoiceNo'].isnull()]

# Count the number of rows with null/nan values in 'InvoiceNo' column
count_null_invoice_numbers = null_invoice_numbers.shape[0]

if count_null_invoice_numbers > 0:
    print("Rows with null/nan values in 'InvoiceNo' column:")
    for index, row in null_invoice_numbers.iterrows():
        print(f"Index: {index}, InvoiceNo: {row['InvoiceNo']}")
else:
    print("No rows with null/nan values in 'InvoiceNo' column.")

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('projekt/csv/retail_2022_final.csv')

# Check for null/nan values in the 'InvoiceNo' column
null_invoice_numbers = df[df['InvoiceNo'].isnull()]

# Count the number of rows with null/nan values in 'InvoiceNo' column
count_null_invoice_numbers = null_invoice_numbers.shape[0]

print("Number of rows with null/nan values in 'InvoiceNo' column:", count_null_invoice_numbers)

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('projekt/csv/retail_2023_final.csv')

# Check for null/nan values in the 'InvoiceNo' column
null_invoice_numbers = df[df['InvoiceNo'].isnull()]

# Count the number of rows with null/nan values in 'InvoiceNo' column
count_null_invoice_numbers = null_invoice_numbers.shape[0]

print("Number of rows with null/nan values in 'InvoiceNo' column:", count_null_invoice_numbers)