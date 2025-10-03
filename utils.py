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

def cleanup_schema(connection = None):
    connection_passed = connection is not None
    if not connection_passed:
        connection = get_connection()

    cursor = connection.cursor()

    sql_statement = """
    DROP TABLE IF EXISTS tweet CASCADE;
    DROP TABLE IF EXISTS user_account CASCADE;
    DROP TABLE IF EXISTS place CASCADE;
    DROP TABLE IF EXISTS hashtag CASCADE;
    DROP TABLE IF EXISTS unwound_url CASCADE;
    DROP TABLE IF EXISTS url CASCADE;
    DROP TABLE IF EXISTS media CASCADE;
    DROP TABLE IF EXISTS user_mention CASCADE;
    """

    try:
        cursor.execute(sql_statement)
        connection.commit()
        print("Schema cleaned up successfully.")
    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        if not connection_passed:
            connection.close()