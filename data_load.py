import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv

load_dotenv()

CSV_FILE = "/Users/macos/Downloads/1000000_Sales_Recordscleaned.csv"

conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
         port=os.getenv("DB_PORT", "5432")
)

cur = conn.cursor()

df = pd.read_csv(CSV_FILE)


df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')

df.dropna(subset=['order_id'], inplace=True)

insert_query = """
INSERT INTO sales_data (
    region, country, item_type, sales_channel, order_priority,
    order_date, order_id, ship_date, units_sold,
    unit_price, unit_cost, total_revenue, total_cost, total_profit
)
VALUES (
    %(region)s, %(country)s, %(item_type)s, %(sales_channel)s, %(order_priority)s,
    %(order_date)s, %(order_id)s, %(ship_date)s, %(units_sold)s,
    %(unit_price)s, %(unit_cost)s, %(total_revenue)s, %(total_cost)s, %(total_profit)s
)
ON CONFLICT (order_id) DO NOTHING;
"""

data = df.to_dict(orient="records")

try:
    execute_batch(cur, insert_query, data, page_size=1000)
    conn.commit()
    print(f"Inserted rows: {cur.rowcount}")
except Exception as e:
    conn.rollback()
    print("Error occurred:", e)
finally:
    cur.close()
    conn.close()
