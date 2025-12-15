# app.py
import json
import os
import psycopg2
import pandas as pd
import streamlit as st
from groq import Groq
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# Load schema
schema = json.load(open("schema.json"))

# Functions
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
    """Remove Markdown fences from LLM-generated SQL"""
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

def check_safety(sql: str):
    """Prevent destructive SQL commands"""
    forbidden = ["drop", "delete", "truncate", "update", "insert"]
    if any(word in sql.lower() for word in forbidden):
        raise ValueError("Unsafe SQL detected!")

# Streamlit UI
st.title("LLM SQL Agent for PostgreSQL")
question = st.text_input("Ask a question about your sales_data:", "")

if st.button("Run Query") and question.strip() != "":
    try:
        # 1. Generate SQL
        sql_raw = generate_sql(question)
        sql_clean = clean_sql(sql_raw)

        # 2. Safety check
        check_safety(sql_clean)

        # 3. Execute
        df = execute_sql(sql_clean)

        # 4. Show generated SQL
        st.subheader("Generated SQL")
        st.code(sql_clean, language="sql")

        # 5. Show results
        st.subheader("Query Results")
        st.dataframe(df)

        # 6. Optional: chart if numeric columns exist
        numeric_cols = df.select_dtypes(include="number").columns
        # Chart or Metric
        if df.shape[0] == 1 and df.shape[1] == 1:
            st.metric(label="Result", value=df.iloc[0,0])
        else:
            numeric_cols = df.select_dtypes(include="number").columns
            if len(numeric_cols) > 0:
                st.subheader("Chart")
                st.bar_chart(df.set_index(df.columns[0])[numeric_cols[0]])

    except Exception as e:
        st.error(f"Error: {e}")

   

