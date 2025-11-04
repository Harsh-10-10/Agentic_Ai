import os
import databricks.sql
import json
from dotenv import load_dotenv

def get_databricks_connection():
    """
    Creates and returns a Databricks SQL connection object.
    """
    load_dotenv()
    
    hostname = os.getenv("DB_HOST")
    http_path = os.getenv("DB_PATH")
    token = os.getenv("DB_TOKEN")

    if not all([hostname, http_path, token]):
        print("Error: Databricks credentials not found in .env file.")
        return None

    try:
        connection = databricks.sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=token
        )
        print("Connection to Databricks successful.")
        return connection
    except Exception as e:
        print(f"Error connecting to Databricks: {e}")
        return None

def list_all_tables() -> str:
    """
    Connects to Databricks and lists all tables in the 'default' schema.
    This is the main tool function for your Autogen agent.
    
    Returns:
        A JSON string of the tables found, or an error message.
    """
    
    # Get a connection
    connection = get_databricks_connection()
    if connection is None:
        return json.dumps({"error": "Failed to create Databricks connection."})

    try:
        with connection.cursor() as cursor:
            # We use 'default' because that's the schema
            # you selected when uploading files.
            cursor.execute("SHOW TABLES IN default")
            tables = cursor.fetchall()

        # Format the output for the agent
        # tables will look like [Row(database='default', tableName='hr_employees', ...), ...]
        table_names = [table.tableName for table in tables]
        
        return json.dumps({
            "catalog": "main",
            "schema": "default",
            "tables": table_names
        })

    except Exception as e:
        return json.dumps({"error": f"Failed to list tables: {e}"})
    
    finally:
        # Always close the connection
        if connection:
            connection.close()

# --- This is how you can test the function directly ---
if __name__ == "__main__":
    
    # 1. Install the connector
    # pip install databricks-sql-connector
    
    # 2. Make sure your .env file is set
    
    # 3. Upload a file (like 'hr_employees') to the 'default' schema
    #    in the Databricks UI
    
    # 4. Run this script
    print("Testing the 'list_all_tables' tool...")
    table_list_json = list_all_tables()
    
    print("\n--- Tool Output (JSON) ---")
    print(table_list_json)
    
    # Pretty print the JSON
    parsed = json.loads(table_list_json)
    print("\n--- Parsed Output ---")
    print(json.dumps(parsed, indent=2))