import os
from dotenv import load_dotenv
import psycopg2

def get_connection():
    load_dotenv()
    db_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }

    return psycopg2.connect(**db_params)


def build_schema(schema_file_name: str = 'schema.sql', connection = None):
    connection_passed = connection is not None
    if not connection_passed:
        connection = get_connection()

    cursor = connection.cursor()

    with open(schema_file_name, 'r') as f:
        sql_statement = f.read()

    try:
        cursor.execute(sql_statement)
        connection.commit()
        print("Schema created successfully.")
    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        if not connection_passed:
            connection.close()




