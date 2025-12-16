print("python file is running")

import psycopg2 
conn=psycopg2.connect(
    host="localhost",
    database="LLMDB",
    user="postgres",
    password="2005",
)
print("DB is connected succesfully")
conn.close()
