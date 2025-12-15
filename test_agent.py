from llm_sql import generate_sql
from db_runner import run_query

question = "Top 5 countries by total revenue"

sql = generate_sql(question)
print("\nGenerated SQL:\n", sql)

df = run_query(sql)
print("\nResult:\n", df.head())
print("over")