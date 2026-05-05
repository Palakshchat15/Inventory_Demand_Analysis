import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

def run_sql_file(file_path):
    DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'inventory_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')

    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    print(f"Executing SQL from {file_path}...")
    with open(file_path, 'r') as file:
        sql = file.read()

    try:
        with engine.connect() as connection:
            connection.execute(text(sql))
            connection.commit()
            print(f"Successfully executed {file_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_sql_file(sys.argv[1])
    else:
        print("Please provide a SQL file path.")
