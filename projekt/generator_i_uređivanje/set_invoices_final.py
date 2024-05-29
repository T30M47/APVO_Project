import pandas as pd
import random

def generate_random_subgroup(group, num_invoices_needed):
    # Get available rows
    available_rows = group.index.tolist()
    
    # Initialize list to store subgroup sizes
    subgroup_sizes = []
    
    # Calculate the total number of rows
    total_rows = len(available_rows)
    
    # Calculate the average size of each subgroup
    avg_subgroup_size = total_rows // num_invoices_needed
    remainder = total_rows % num_invoices_needed
    
    # Distribute the remainder among the subgroups
    for _ in range(num_invoices_needed):
        subgroup_size = avg_subgroup_size
        if remainder > 0:
            subgroup_size += 1
            remainder -= 1
        subgroup_sizes.append(subgroup_size)
    
    for _ in range(random.randint(15, 30)):
        # Calculate 0.2% of average subgroup size for further variations
        variation = int(round((avg_subgroup_size * 0.25)))

        subgroup_to_decrease = random.randint(0, num_invoices_needed - 1)

        hh = subgroup_sizes[subgroup_to_decrease] - variation
        
        while hh <= 0:
            # Randomly choose a subgroup to decrease its size
            subgroup_to_decrease = random.randint(0, num_invoices_needed - 1)
            hh = subgroup_sizes[subgroup_to_decrease] - variation
        
        subgroup_sizes[subgroup_to_decrease] -= variation
        
        # Find the index of the subgroup to increase its size (excluding the chosen subgroup)
        subgroups_to_increase = [i for i in range(num_invoices_needed) if i != subgroup_to_decrease]
        
        # Randomly choose one of the remaining subgroups to increase its size
        subgroup_to_increase = random.choice(subgroups_to_increase)
        
        # Increase the size of the chosen subgroup by 0.2%
        subgroup_sizes[subgroup_to_increase] += variation
    
    # Return subgroup sizes
    return subgroup_sizes


def generate_invoice_numbers(df, invoice_counts, used_invoice_numbers):
    # Function to generate a random invoice number with 6 digits
    def generate_invoice_number():
        return random.randint(100000, 999999)

    # Convert 'InvoiceDate' column to datetime if it's not already in datetime format
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%m-%d')
    grouped = df.groupby('InvoiceDate')

    for date, group in grouped:
        invoice_numbers = set()
        # Determine the number of invoices needed for this date from the invoice_counts DataFrame
        try:
            num_invoices_needed = invoice_counts.loc[invoice_counts['InvoiceDate'] == date, 'Count'].iloc[0]
        except IndexError:
            # Calculate the average count of invoices for the month
            month = pd.to_datetime(date, format='%m-%d').month
            average_count = invoice_counts[invoice_counts['InvoiceDate'].str.startswith(str(month).zfill(2))]['Count'].mean()
            num_invoices_needed = round(average_count)
        
        # Generate invoice numbers for this date
        while len(invoice_numbers) < num_invoices_needed:
            invoice_number = generate_invoice_number()
            if invoice_number not in used_invoice_numbers:
                invoice_numbers.add(invoice_number)
                used_invoice_numbers.add(invoice_number)  # Add the invoice number to used set

        subgroup_sizes = generate_random_subgroup(group, num_invoices_needed)
        
        # Assign invoice numbers to rows, avoiding rows that already have an invoice number
        assigned_rows = set()
                
        for i, invoice_number in enumerate(invoice_numbers):
            available_rows = [idx for idx in group.index if idx not in assigned_rows]
            # Randomly choose a subset of rows corresponding to this date to assign the current invoice number
            rows_to_assign = random.sample(available_rows, subgroup_sizes[i])
            assigned_rows.update(rows_to_assign)
            # Assign invoice numbers to the selected rows
            group.loc[group.index.isin(rows_to_assign), 'InvoiceNo'] = int(invoice_number)

        df.loc[group.index, 'InvoiceNo'] = group['InvoiceNo']

    return df

used_invoice_numbers = set()

# Load the CSV files and process them
for year in range(2021, 2024):
    filename = f'projekt/csv/retail_{year}.csv'
    filename_old = f'projekt/csv/retail_2020.csv'
    output_filename = f'projekt/csv/retail_{year}_final.csv'
    output_filename_old = f'projekt/csv/retail_{year - 1}_final.csv'

    df = pd.read_csv(filename)

    df_old = pd.read_csv(filename_old)
    used_invoice_numbers.update(set(df_old['InvoiceNo']))
    df_old['InvoiceDate'] = pd.to_datetime(df_old['InvoiceDate'])
    invoice_counts = df_old.groupby(df_old['InvoiceDate'].dt.strftime('%m-%d'))['InvoiceNo'].nunique().reset_index(name='Count')

    # Generate invoice numbers for the current DataFrame
    df = generate_invoice_numbers(df, invoice_counts, used_invoice_numbers)

    # Save the modified DataFrame back to CSV
    df.to_csv(output_filename, index=False)
