import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'inventory_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

import urllib.parse

def setup_database():
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    
    schema_path = os.path.join('sql', '01_schema_creation.sql')
    
    if not os.path.exists(schema_path):
        print(f"Error: {schema_path} not found.")
        return

    print(f"Executing schema from {schema_path}...")
    with open(schema_path, 'r') as file:
        schema_sql = file.read()

    # Split by semicolon to execute commands individually if needed, 
    # but SQLAlchemy's execute can often handle blocks.
    # However, for simplicity and error handling, we'll use a transaction.
    try:
        with engine.connect() as connection:
            # We need to execute the script. 
            # Note: views and complex blocks might need special handling depending on the driver,
            # but usually, this works for standard DDL.
            connection.execute(text(schema_sql))
            connection.commit()
            print("Database schema created successfully.")
    except Exception as e:
        print(f"Error executing schema: {e}")

if __name__ == "__main__":
    setup_database()
