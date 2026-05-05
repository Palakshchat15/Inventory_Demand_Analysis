import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'inventory_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

import urllib.parse

def get_engine():
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def load_csv_to_db(file_path, table_name, engine, chunksize=100000):
    print(f"Loading {file_path} into table {table_name}...")
    start_time = time.time()
    
    # Use different encoding for Online Retail
    encoding = 'ISO-8859-1' if 'online_retail' in file_path else 'utf-8'
    
    # Read and load in chunks to save memory
    for i, chunk in enumerate(pd.read_csv(file_path, encoding=encoding, chunksize=chunksize)):
        if 'online_retail' in file_path:
            chunk.columns = [c.lower().replace(' ', '_') for c in chunk.columns]
            # Mapping specific inconsistencies
            chunk = chunk.rename(columns={'invoiceno': 'invoice_no', 'stockcode': 'stock_code', 'unitprice': 'unit_price', 'customerid': 'customer_id', 'invoicedate': 'invoice_date'})
        
        chunk.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"  Processed chunk {i+1} ({(i+1)*chunksize} rows)...")
    
    end_time = time.time()
    print(f"Successfully loaded {table_name} in {end_time - start_time:.2f} seconds.")

def main():
    engine = get_engine()
    raw_data_dir = os.path.join('data', 'raw')
    
    # Configuration: (File Name, Table Name)
    datasets = [
        ('online_retail.csv', 'retail_sales_raw'),
        ('aisles.csv', 'aisles'),
        ('departments.csv', 'departments'),
        ('products.csv', 'instacart_products'),
        ('orders.csv', 'instacart_orders'),
        ('order_products__train.csv', 'instacart_order_items'),
        # Note: order_products__prior is very large, maybe skip or load last
        ('order_products__prior.csv', 'instacart_order_items_prior')
    ]
    
    for file_name, table_name in datasets:
        file_path = os.path.join(raw_data_dir, file_name)
        if os.path.exists(file_path):
            try:
                load_csv_to_db(file_path, table_name, engine)
            except Exception as e:
                print(f"Error loading {file_name}: {e}")
        else:
            print(f"Warning: {file_name} not found in {raw_data_dir}")

if __name__ == "__main__":
    main()
