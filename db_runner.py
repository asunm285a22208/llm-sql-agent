import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def run_query(sql):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df
print('completed')