import os
import sqlalchemy as sa
import json
from dotenv import load_dotenv

def get_databricks_engine():
    """
    Creates and returns a SQLAlchemy engine for Databricks.
    Pulls all credentials from your .env file.
    """
    load_dotenv()
    
    # These must match your .env file
    hostname = os.getenv("DB_HOST")
    http_path = os.getenv("DB_PATH")
    token = os.getenv("DB_TOKEN")

    if not all([hostname, http_path, token]):
        print("Error: Databricks credentials (DB_HOST, DB_PATH, DB_TOKEN) not found in .env file.")
        return None

    try:
        # --- THIS IS THE FIX ---
        # Changed 'catalog=hive_metastore' to 'catalog=workspace'
        connection_string = (
            f"databricks://token:{token}@{hostname}?"
            f"http_path={http_path}&"
            f"catalog=workspace&"
            f"schema=default"
        )
        
        engine = sa.create_engine(connection_string)
        
        # Test the connection
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        
        print("SQLAlchemy engine for Databricks (workspace.default) created successfully.")
        return engine
        
    except Exception as e:
        print(f"Error creating Databricks SQLAlchemy engine: {e}")
        return None

def list_all_tables() -> str:
    """
    Connects to Databricks via SQLAlchemy and lists all tables 
    in the 'workspace.default' schema (pre-configured in the engine).
    
    This is the function your agent will use as a tool.
    
    Returns:
        A JSON string of the tables found, or an error message.
    """
    
    print("Connecting to Databricks with SQLAlchemy to list tables...")
    engine = get_databricks_engine()
    if engine is None:
        return json.dumps({"error": "Failed to create Databricks connection."})

    try:
        inspector = sa.inspect(engine)
        
        # We don't need schema='default' because the engine now knows
        table_names = inspector.get_table_names()
        
        print(f"Success! Found {len(table_names)} tables in workspace.default.")
        return json.dumps({
            "catalog": "workspace",
            "schema": "default",
            "tables_found": table_names
        })

    except Exception as e:
        return json.dumps({"error": f"Failed to list tables: {e}"})
    
    finally:
        # Dispose of the engine to close all connections
        if engine:
            engine.dispose()

# --- This part lets you test the file directly ---
if __name__ == "__main__":
    
    print("--- Running Databricks Connection Test ---")
    
    # Make sure your SQL Warehouse is running (green dot)
    table_list_json = list_all_tables()
    
    print("\n--- Tool Output (JSON) ---")
    print(table_list_json)
    
    # Pretty print the JSON
    parsed = json.loads(table_list_json)
    print("\n--- Parsed Output ---")
    print(json.dumps(parsed, indent=2))

