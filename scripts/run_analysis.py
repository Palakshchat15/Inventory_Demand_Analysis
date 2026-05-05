import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'inventory_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

def get_engine():
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def run_analysis():
    engine = get_engine()
    
    # Define queries
    queries = {
        "ABC Analysis (Top 10)": """
            WITH product_revenue AS (
                SELECT stock_code, SUM(line_total) as revenue
                FROM fct_retail_sales
                GROUP BY stock_code
            ),
            revenue_ranked AS (
                SELECT stock_code, revenue,
                SUM(revenue) OVER (ORDER BY revenue DESC) / SUM(revenue) OVER () as cumulative_pct
                FROM product_revenue
            )
            SELECT stock_code, revenue,
            CASE WHEN cumulative_pct <= 0.80 THEN 'A' WHEN cumulative_pct <= 0.95 THEN 'B' ELSE 'C' END as abc_category
            FROM revenue_ranked LIMIT 10;
        """,
        "Market Basket Analysis (Top 10 pairs)": """
            WITH order_pairs AS (
                SELECT a.product_id as prod_a, b.product_id as prod_b
                FROM instacart_order_items a
                JOIN instacart_order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
                LIMIT 100000 
            )
            SELECT pa.product_name as product_1, pb.product_name as product_2, COUNT(*) as times_bought_together
            FROM order_pairs op
            JOIN instacart_products pa ON op.prod_a = pa.product_id
            JOIN instacart_products pb ON op.prod_b = pb.product_id
            GROUP BY pa.product_name, pb.product_name
            ORDER BY times_bought_together DESC LIMIT 10;
        """,
        "Peak Order Hours (Instacart)": """
            SELECT order_hour_of_day, COUNT(order_id) as total_orders
            FROM instacart_orders
            GROUP BY order_hour_of_day
            ORDER BY total_orders DESC LIMIT 5;
        """
    }
    
    print("\n" + "="*50)
    print("RUNNING INTELLIGENT INVENTORY & DEMAND ANALYTICS")
    print("="*50)
    
    for title, query in queries.items():
        print(f"\n--- {title} ---")
        try:
            df = pd.read_sql(query, engine)
            print(df.to_string(index=False))
        except Exception as e:
            print(f"Error running {title}: {e}")

if __name__ == "__main__":
    run_analysis()
