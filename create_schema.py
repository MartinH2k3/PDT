import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

db_params = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

schema_file = 'schema.sql'
with open(schema_file, 'r') as f:
    sql_statement = f.read()

connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

try:
    cursor.execute(sql_statement)
    connection.commit()
    print("Schema created successfully.")
except Exception as e:
    connection.rollback()
    print(f"An error occurred: {e}")
finally:
    cursor.close()
    connection.close()