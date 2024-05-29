import pandas as pd

# List of CSV files to merge
csv_files = ['projekt/csv/retail_2020.csv', 'projekt/csv/retail_2021_years.csv', 'projekt/csv/retail_2022_years.csv', 'projekt/csv/retail_2023_years.csv']

# Initialize an empty list to store DataFrames
dfs = []

# Iterate over each CSV file
for file in csv_files:
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file)
    # Append the DataFrame to the list
    dfs.append(df)

# Concatenate all DataFrames in the list along the rows
merged_df = pd.concat(dfs, ignore_index=True)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('projekt/csv/retail_all.csv', index=False)