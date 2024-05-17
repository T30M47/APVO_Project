import pandas as pd

# Assuming your data is in a CSV file named 'invoices.csv'
data = pd.read_csv('generator/csv/retail_cleaned.csv')

# Count the number of unique invoice numbers
num_unique_invoices = data['InvoiceNo'].nunique()

print("Number of unique invoice numbers in the data:", num_unique_invoices)
import pandas as pd

# Assuming your data is in a CSV file named 'invoices.csv'
data = pd.read_csv('csv/retail_2021_final.csv')

# Count the number of unique invoice numbers
num_unique_invoices = data['InvoiceNo'].nunique()

print("Number of unique invoice numbers in the data:", num_unique_invoices)

# Assuming your data is in a CSV file named 'invoices.csv'
data = pd.read_csv('csv/retail_2022_final.csv')

# Count the number of unique invoice numbers
num_unique_invoices = data['InvoiceNo'].nunique()

print("Number of unique invoice numbers in the data:", num_unique_invoices)

# Assuming your data is in a CSV file named 'invoices.csv'
data = pd.read_csv('csv/retail_2023_final.csv')

# Count the number of unique invoice numbers
num_unique_invoices = data['InvoiceNo'].nunique()

print("Number of unique invoice numbers in the data:", num_unique_invoices)



# Load the CSV file with invoices
df_invoices = pd.read_csv('csv/retail_2023_final.csv')

# Group by InvoiceNo and list the dates for each invoice number
invoices_dates = df_invoices.groupby('InvoiceNo')['InvoiceDate'].apply(set)

# Dictionary to store dates for each invoice number
invoice_dates_dict = {}

# Populate the dictionary
for invoice_number, dates in invoices_dates.items():
    for date in dates:
        if invoice_number in invoice_dates_dict:
            invoice_dates_dict[invoice_number].append(date)
        else:
            invoice_dates_dict[invoice_number] = [date]

# Check if any invoice number appears in more than one date
duplicate_invoices = {invoice_number: dates for invoice_number, dates in invoice_dates_dict.items() if len(dates) > 1}

if not duplicate_invoices:
    print("No invoice numbers assigned to different dates.")
else:
    print("Invoice numbers assigned to different dates:")
    for invoice_number, dates in duplicate_invoices.items():
        print(f"Invoice Number: {invoice_number}, Dates: {', '.join(dates)}")

# Load the retail_2021.csv file
"""df = pd.read_csv('csv/retail_2020.csv')

# Convert 'InvoiceDate' column to datetime if it's not already in datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Group by 'InvoiceDate' and count unique 'InvoiceNo' within each group
invoice_counts = df.groupby(df['InvoiceDate'].dt.strftime('%m-%d'))['InvoiceNo'].nunique().reset_index(name='Count')

# Summing the 'Count' column
total_count = invoice_counts['Count'].sum()

print("Total count:", total_count)

# Load the retail_2021.csv file
df = pd.read_csv('csv/retail_2021_final.csv')

# Convert 'InvoiceDate' column to datetime if it's not already in datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m-%d')

# Group by 'InvoiceDate' and count unique 'InvoiceNo' within each group
invoice_counts_2 = df.groupby(df['InvoiceDate'].dt.strftime('%m-%d'))['InvoiceNo'].nunique().reset_index(name='Count')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Merge the DataFrames on 'InvoiceDate'
merged_df = pd.merge(invoice_counts, invoice_counts_2, on='InvoiceDate',suffixes=('_1', '_2'))

#print(merged_df)



# Load the retail_2021.csv file
df = pd.read_csv('csv/retail_2021_with_invoices_5.csv')

# Convert 'InvoiceDate' column to datetime if it's not already in datetime format
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m-%d')

# Group by 'InvoiceDate' and count unique 'InvoiceNo' within each group
invoice_counts_3 = df.groupby(df['InvoiceDate'].dt.strftime('%m-%d'))['InvoiceNo'].nunique().reset_index(name='Count')

print(invoice_counts_3)

# Summing the 'Count' column
total_count = invoice_counts['Count'].sum()

print("Total count:", total_count)"""