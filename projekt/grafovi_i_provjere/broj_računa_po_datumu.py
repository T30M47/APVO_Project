import pandas as pd

# Učitajte CSV datoteku za određenu godinu
df = pd.read_csv('projekt/csv/retail_2022_final.csv')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Pretvorite InvoiceDate u tip datuma
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m-%d')

# Grupirajte po datumu računa (samo mjesec i dan) i broju računa te izračunajte broj pojavljivanja svakog računa unutar grupe
invoice_counts_per_group = df.groupby([df['InvoiceDate'].dt.strftime('%m-%d'), 'InvoiceNo']).size().reset_index(name='Count')

# Prikaz broja računa svakog broja po grupi
print(invoice_counts_per_group)
