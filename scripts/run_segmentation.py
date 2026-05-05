import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

def run_segmentation():
    DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'inventory_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')

    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    print("\n--- Running Customer Segmentation (RFM) ---")
    
    with open('sql/04_customer_segmentation.sql', 'r') as f:
        # Split by semicolon but ignore comments
        sql_content = f.read()
        queries = sql_content.split(';')

    with engine.begin() as conn:
        for query in queries:
            if query.strip():
                conn.execute(text(query))
                print("Executed step...")
    
    print("✅ Segmentation Complete! Table 'customer_segments' is ready.")

if __name__ == "__main__":
    run_segmentation()
