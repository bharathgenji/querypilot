import psycopg2

conn = psycopg2.connect(
    dbname="Adventureworks",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

cursor.execute("""
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
""")
schemas = cursor.fetchall()
print("Schemas:", schemas)

conn.close()
