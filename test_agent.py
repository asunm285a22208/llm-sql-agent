from llm_sql import generate_sql,clean_sql
from db_runner import run_query


question = "Top 5 countries by total revenue"

sql_raw= generate_sql(question)
print("\nGenerated SQL:\n", sql_raw)

sql_clean = clean_sql(sql_raw)
print("\nGenerated SQL (cleaned):\n", sql_clean)


df = run_query(sql_clean)
print("\nResult:\n", df.head())
print(df)