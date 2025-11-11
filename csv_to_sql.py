import os
import pandas as pd
import pymysql

csv_files = [
    ('customers.csv', 'customers'),
    ('geolocation.csv', 'geolocation'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),        # fixed
    ('products.csv', 'products'),
    ('order_items.csv', 'order_items'),
    ('payments.csv', 'payments')
]

conn = pymysql.connect(
    host='127.0.0.1',
    user='mohit',
    password='Mohit@78271',
    database='ecommerce'
)

cursor = conn.cursor()

folder_path = '/Users/mac/Desktop/archiven/'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)

    df = pd.read_csv(file_path)

    df = df.where(pd.notnull(df), None)

    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    placeholders = ', '.join(['%s'] * len(df.columns))
    col_list = ', '.join(f'`{c}`' for c in df.columns)
    insert_sql = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"

    data = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False, name=None)]
    if data:
        cursor.executemany(insert_sql, data)

    conn.commit()

cursor.close()
conn.close()
