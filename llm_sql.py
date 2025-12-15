import json
import os
import pandas as pd
import psycopg2
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

schema = json.load(open("schema.json"))

def generate_sql(question: str) -> str:
    prompt = f"""
You are an expert PostgreSQL data analyst.

Rules:
- Use ONLY the table sales_data
- Use ONLY columns from schema
- Generate SAFE SELECT queries only
- Return ONLY SQL (no explanation)

Schema:
{json.dumps(schema, indent=2)}

User Question:
{question}
"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content.strip()

def clean_sql(sql: str) -> str:
    return sql.replace("```sql", "").replace("```", "").strip()

def execute_sql(sql: str) -> pd.DataFrame:
    conn = psycopg2.connect(
        host="localhost",
        database="LLMDB",
        user="postgres",
        password="2005",
        port="5432"
    )
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df

if __name__ == "__main__":
    question = "Top 5 countries by total revenue"

    # 1. Generate SQL from LLM
    sql_raw = generate_sql(question)

    # 2. Clean Markdown fences
    sql_clean = clean_sql(sql_raw)
    print("Generated SQL (cleaned):")
    print(sql_clean)
    
    # 3. Safety check before execution
    forbidden = ["drop", "delete", "truncate", "update", "insert"]
    if any(word in sql_clean.lower() for word in forbidden):
        raise ValueError("Unsafe SQL detected! Execution stopped.")


    # 3. Execute SQL safely
    df = execute_sql(sql_clean)
    print("\nQuery Result:")
    print(df)
print('completed')