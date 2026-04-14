import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="health_dw",
    user="postgres",
    password="test"
)

print("OK connection")
conn.close()