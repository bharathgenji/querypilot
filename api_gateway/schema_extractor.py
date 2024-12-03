# api_gateway/schema_extractor.py
import psycopg2
import os
import logging

logger = logging.getLogger(__name__)


def standardize_table_names(sql_query, mapped_schema):
    """
    Replace plain table names in the SQL query with fully qualified names from the schema.

    Args:
        sql_query (str): Generated SQL query.
        mapped_schema (dict): Schema information with fully qualified table names.

    Returns:
        str: SQL query with standardized table names.
    """
    for table_name, table_info in mapped_schema["tables"].items():
        short_table_name = table_name.split(".")[-1]  # Get the base table name (e.g., "employee")
        sql_query = sql_query.replace(f"{short_table_name} ", f"{table_name} ")
        sql_query = sql_query.replace(f"{short_table_name}(", f"{table_name}(")  # For subqueries or joins

    return sql_query

def print_dict_keys(d, prefix=""):
    """
    Recursively print keys of a dictionary to understand its structure.
    
    Args:
        d (dict): The dictionary to inspect.
        prefix (str): Prefix for nested keys to show hierarchy.
    """
    if isinstance(d, dict):
        for key in d.keys():
            print(f"{prefix}{key}")
            # Recursively inspect nested dictionaries
            print_dict_keys(d[key], prefix=prefix + "    ")
    elif isinstance(d, list):
        print(f"{prefix}[List]")
    else:
        print(f"{prefix}{type(d).__name__}")



def map_relevant_schemas_to_tables(relevant_schemas, schema_info):
    """
    Match relevant schemas and tables with the schema dictionary and retrieve their details.

    Args:
        relevant_schemas (list): List of relevant schema details as dictionaries.
        schema_info (dict): Complete schema information from the database.

    Returns:
        dict: Filtered schema details containing tables and their relationships.
    """
    filtered_schema = {"tables": {}}

    for schema_item in relevant_schemas:
        # Extract schema and table name from the relevant schema item
        schema_name = schema_item.get("schema")
        description = schema_item.get("description", "")
        
        # Extract the table name from the description (e.g., "Column subtotal in table purchaseorderheader")
        table_name = None
        if "in table" in description:
            table_name = description.split("in table")[-1].strip()

        if not schema_name or not table_name:
            continue  # Skip if schema or table name is missing

        # Check if the schema exists in schema_info
        if schema_name in schema_info.get("schemas", {}):
            schema_data = schema_info["schemas"][schema_name]

            # Check if the table exists in the schema
            if table_name in schema_data.get("tables", {}):
                # Add the table and its relationships to the filtered schema
                filtered_schema["tables"][f"{schema_name}.{table_name}"] = {
                    "columns": schema_data["tables"][table_name].get("columns", {}),
                    "relations": schema_data["tables"][table_name].get("relations", {}),
                }

    return filtered_schema



def flatten_schema_info(schema_info):
    schema_elements = []

    try:
        for schema_name, schema_data in schema_info["schemas"].items():
            for table_name, table_data in schema_data["tables"].items():
                # Process columns
                for column_name, column_data in table_data.get("columns", {}).items():
                    schema_elements.append({
                        "type": "column",
                        "schema": schema_name,
                        "table": table_name,
                        "name": column_name,
                        "description": f"Column {column_name} of type {column_data.get('data_type', 'unknown')} in table {table_name}",
                        "data_type": column_data.get("data_type", "unknown"),
                        "is_nullable": column_data.get("is_nullable", "unknown")
                    })

                # Process primary keys
                for primary_key in table_data.get("constraints", {}).get("primary_key", []):
                    schema_elements.append({
                        "type": "primary_key",
                        "schema": schema_name,
                        "table": table_name,
                        "column": primary_key,
                        "description": f"Primary key on column {primary_key} in table {table_name}"
                    })

                # Process other constraints similarly...
    except Exception as e:
        print(f"Error while flattening schema info: {e}")

    return schema_elements



def get_schema():
    schema_info = {"schemas": {}}

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

    try:
        cursor = conn.cursor()

        # Step 1: Retrieve all schemas except system schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        print("List of Schemas : ",schemas)

        for schema in schemas:
            schema_info["schemas"][schema] = {"tables": {}}

            # Step 2: Retrieve table and column details for each schema
            cursor.execute("""
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s;
            """, [schema])
            columns = cursor.fetchall()

            for table_name, column_name, data_type, is_nullable in columns:
                if table_name not in schema_info["schemas"][schema]["tables"]:
                    schema_info["schemas"][schema]["tables"][table_name] = {
                        "columns": {},
                        "constraints": {
                            "primary_key": [],
                            "foreign_keys": [],
                            "unique": [],
                            "check": [],
                        }
                    }
                schema_info["schemas"][schema]["tables"][table_name]["columns"][column_name] = {
                    "data_type": data_type,
                    "is_nullable": is_nullable
                }

            # Step 3: Retrieve constraints (primary key, foreign key, unique, and check)
            cursor.execute("""
                SELECT
                    tc.constraint_type,
                    kcu.table_name AS table_name,
                    kcu.column_name AS column_name,
                    ccu.table_name AS related_table,
                    ccu.column_name AS related_column,
                    tc.constraint_name AS constraint_name
                FROM information_schema.table_constraints AS tc
                LEFT JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                LEFT JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_schema = %s;
            """, [schema])
            constraints = cursor.fetchall()

            for (
                constraint_type,
                table_name,
                column_name,
                related_table,
                related_column,
                constraint_name,
            ) in constraints:
                if table_name in schema_info["schemas"][schema]["tables"]:
                    if constraint_type == "PRIMARY KEY":
                        schema_info["schemas"][schema]["tables"][table_name]["constraints"]["primary_key"].append(column_name)
                    elif constraint_type == "FOREIGN KEY":
                        schema_info["schemas"][schema]["tables"][table_name]["constraints"]["foreign_keys"].append({
                            "column": column_name,
                            "related_table": related_table,
                            "related_column": related_column,
                            "constraint_name": constraint_name
                        })
                    elif constraint_type == "UNIQUE":
                        schema_info["schemas"][schema]["tables"][table_name]["constraints"]["unique"].append({
                            "column": column_name,
                            "constraint_name": constraint_name
                        })

            # Step 4: Retrieve check constraints
            cursor.execute("""
                SELECT 
                    tc.table_name,
                    tc.constraint_name,
                    cc.check_clause
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.check_constraints AS cc
                    ON tc.constraint_name = cc.constraint_name
                WHERE tc.table_schema = %s AND tc.constraint_type = 'CHECK';
            """, [schema])
            check_constraints = cursor.fetchall()

            for table_name, constraint_name, check_clause in check_constraints:
                if table_name in schema_info["schemas"][schema]["tables"]:
                    schema_info["schemas"][schema]["tables"][table_name]["constraints"]["check"].append({
                        "constraint_name": constraint_name,
                        "check_clause": check_clause
                    })

        return schema_info

    except Exception as e:
        print(f"Error retrieving schema information: {e}")
        return None
    finally:
        conn.close()


