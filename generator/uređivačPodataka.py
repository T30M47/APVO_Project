import pandas as pd

def clean_data(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Drop rows with NaN values in the Description column
    df.dropna(subset=['Description'], inplace=True)

    # List of descriptions to remove
    descriptions_to_remove = ['Unsaleable, destroyed.', 'ebay', 'thrown away', 'thrown away-can\'t sell.',  'thrown away-can\'t sell', 'damages', 'Thrown away-rusty', 'mailout']  # Add more descriptions as needed

    # Drop rows with specified descriptions
    df = df[~df['Description'].isin(descriptions_to_remove)]

    # Write the cleaned data back to CSV
    cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
    df.to_csv(cleaned_file_path, index=False)

    print(f"Cleaned data saved to: {cleaned_file_path}")

if __name__ == "__main__":
    file_path = 'generator/csv/retail.csv'
    clean_data(file_path)
