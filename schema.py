import psycopg2
import json

conn = psycopg2.connect(
    host="localhost",
    database="LLMDB",
    user="postgres",
    port="5432",
    password="2005"
)

cur = conn.cursor()

cur.execute("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'sales_data'
ORDER BY ordinal_position;
""")
columns = cur.fetchall()

schema = {
    "sales_data": [
        {"column": col, "type": dtype} for col, dtype in columns
    ]
}

cur.close()
conn.close()

with open("schema.json", "w") as f:
    json.dump(schema, f, indent=2)

print("sales_data schema saved to schema.json")