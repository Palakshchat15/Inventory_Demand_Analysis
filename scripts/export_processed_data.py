import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

def export_data():
    DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'inventory_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')

    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    output_dir = os.path.join('data', 'processed')
    
    # Tables to export
    exports = {
        'cleaned_retail_sales.csv': "SELECT * FROM fct_retail_sales",
        'retail_product_catalog.csv': "SELECT * FROM dim_products_retail",
        'instacart_hourly_demand.csv': "SELECT order_hour_of_day, COUNT(order_id) as total_orders FROM instacart_orders GROUP BY order_hour_of_day"
    }

    print("\n--- Exporting Processed Data to CSV ---")
    for file_name, query in exports.items():
        try:
            file_path = os.path.join(output_dir, file_name)
            df = pd.read_sql(query, engine)
            df.to_csv(file_path, index=False)
            print(f"Exported: {file_name} ({len(df):,} rows)")
        except Exception as e:
            print(f"Error exporting {file_name}: {e}")

if __name__ == "__main__":
    export_data()
