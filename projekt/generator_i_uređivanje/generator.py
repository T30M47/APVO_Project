import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# Function to generate a single transaction
def generate_transaction(i, product_counts, weights, top_products, df, sampled_dates):
    sampled_product = None
    if np.random.rand() < 0.35:  # 30% chance of sampling from top products
        sampled_product = np.random.choice(top_products.index, p=top_products.values)
    else:
        sampled_product = np.random.choice(product_counts.index, p=weights)
    
    new_description = df[df['StockCode'] == sampled_product]['Description'].iloc[0]
    new_quantity = np.random.randint(1, 10, size=1)[0]
    new_date = sampled_dates[i] + timedelta(days=np.random.randint(1, 30))
    new_unit_price = df.loc[df['StockCode'] == sampled_product, 'UnitPrice'].iloc[0]
    # new_customer_id = df['CustomerID'].sample(n=1).iloc[0]
    # new_country = df['Country'].sample(n=1).iloc[0]
    
    return [sampled_product, new_description, new_quantity, new_date.strftime('%m/%d/%Y %H:%M'), new_unit_price]
    #, new_customer_id, new_country
def generate_transaction_wrapper(args):
    return generate_transaction(*args)

if __name__ == '__main__':
    # Step 1: Read the CSV file and drop rows with no description
    df = pd.read_csv('projekt/csv/retail_cleaned.csv')
    df.dropna(subset=['Description'], inplace=True)

    # Step 2: Identify the top products sold in the original dataset
    top_n = 55  # Choose the number of top products you want to consider
    top_products = df['StockCode'].value_counts().nlargest(top_n)
    top_products = top_products / top_products.sum()  # Convert counts to probabilities

    # Step 3: Analyze the distribution of products sold
    product_counts = df['StockCode'].value_counts()

    # Step 4: Modify the distribution
    exponent = 2
    epsilon = 1e-6
    weights = (product_counts.values + epsilon) ** (-exponent)
    weights /= weights.sum()

    # Step 5: Generate new transactions
    num_transactions = 450000

    # Sample dates from original data
    original_dates = pd.to_datetime(df['InvoiceDate'])

    # Calculate monthly distribution of transaction dates
    monthly_distribution = original_dates.dt.to_period("M").value_counts(normalize=True)

    # Generate sampled dates based on monthly distribution
    sampled_dates = []
    for _ in range(num_transactions):
        month = np.random.choice(monthly_distribution.index, p=monthly_distribution.values)
        start_date = pd.to_datetime(month.start_time)
        end_date = pd.to_datetime(month.end_time)
        sampled_dates.append(np.random.choice(pd.date_range(start_date, end_date)))

    sampled_dates = pd.to_datetime(sampled_dates)

    # Number of processes to run in parallel (use number of CPU cores)
    num_processes = cpu_count()

    # Create a multiprocessing pool
    pool = Pool(processes=num_processes)

    # Generate transactions in parallel
    args_list = [(i, product_counts, weights, top_products, df, sampled_dates) for i in range(num_transactions)]
    new_transactions = pool.map(generate_transaction_wrapper, args_list)

    # Close the pool to free resources
    pool.close()
    pool.join()

    # Step 6: Create DataFrame from new transactions
    new_df = pd.DataFrame(new_transactions, columns=['StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice'])
    #, 'CustomerID', 'Country'
    # Step 7: Sort the DataFrame by date
    new_df['InvoiceDate'] = pd.to_datetime(new_df['InvoiceDate'])
    new_df.sort_values(by='InvoiceDate', inplace=True)

    # Step 8: Write the generated transactions to a new CSV file
    new_df.to_csv('projekt/csv/cetvrta_godina_55.csv', index=False)
