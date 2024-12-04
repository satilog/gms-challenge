import psycopg2


def execute_sql_script(db_config, script_path):
    """
    Executes the SQL statements in the given script file.

    :param db_config: Dictionary containing database connection parameters.
    :param script_path: Path to the SQL script file.
    """
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            # Read and execute the SQL script
            with open(script_path, "r") as file:
                sql = file.read()
                cursor.execute(sql)
        conn.commit()
        print(f"SQL script {script_path} executed successfully.")
    except Exception as e:
        print(f"Error executing SQL script: {e}")
    finally:
        if conn:
            conn.close()
