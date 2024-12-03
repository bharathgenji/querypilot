# api_gateway/sql_service.py
from sql_metadata import Parser
import logging
from django.db import connection

logger = logging.getLogger(__name__)




import re
from difflib import get_close_matches

import sqlparse
from sentence_transformers import SentenceTransformer

def validate_columns(parsed_query, table_map):
    """
    Validate and correct column names in the SQL query against the schema.
    Args:
        parsed_query (dict): Parsed query components.
        table_map (dict): Mapping of table -> schema details.
    Returns:
        dict: Validated and corrected columns.
    Raises:
        ValueError: If a column cannot be corrected.
    """
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    corrected_columns = {}

    for column in parsed_query["columns"]:
        alias, col_name = (column.split(".") + [None])[:2]
        if alias:
            # Identify the table from the alias
            table_name = next((table for table in table_map if alias == table), None)
            if not table_name:
                raise ValueError(f"Alias '{alias}' does not map to any table.")
        else:
            # Match the column without alias
            table_name = next((table for table in table_map if col_name in table_map[table]["columns"]), None)
            if not table_name:
                raise ValueError(f"Column '{col_name}' not found in schema.")

        # Validate the column against the table's schema
        valid_columns = table_map[table_name]["columns"].keys()
        if col_name not in valid_columns:
            # Use semantic similarity to suggest corrections
            valid_column_embeddings = model.encode(list(valid_columns))
            column_embedding = model.encode([col_name])
            similarities = valid_column_embeddings @ column_embedding.T
            closest_match = max(zip(valid_columns, similarities), key=lambda x: x[1])
            if closest_match[1] < 0.8:  # Threshold for similarity
                raise ValueError(f"Column '{col_name}' cannot be validated or corrected.")
            corrected_columns[column] = f"{alias}.{closest_match[0]}" if alias else closest_match[0]
        else:
            corrected_columns[column] = column

    return corrected_columns

def parse_sql_query(sql_query):
    """
    Parse SQL query to extract table names, columns, and aliases.

    Args:
        sql_query (str): SQL query string.

    Returns:
        dict: Parsed components, including tables and columns.
    """
    parsed = sqlparse.parse(sql_query)[0]
    tokens = parsed.tokens

    components = {
        "tables": [],
        "columns": []
    }

    select_seen = False
    for token in tokens:
        # Detect SELECT clause
        if token.ttype is sqlparse.tokens.DML and token.value.upper() == "SELECT":
            select_seen = True
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
            select_seen = False

        # Handle SELECT columns
        elif select_seen and isinstance(token, sqlparse.sql.IdentifierList):
            for col in token.get_identifiers():
                components["columns"].append(str(col).strip())
        elif select_seen and isinstance(token, sqlparse.sql.Identifier):
            components["columns"].append(str(token).strip())

        # Handle FROM and JOIN tables
        elif not select_seen and isinstance(token, sqlparse.sql.IdentifierList):
            for table in token.get_identifiers():
                components["tables"].append({
                    "name": table.get_real_name(),
                    "alias": table.get_alias()
                })
        elif not select_seen and isinstance(token, sqlparse.sql.Identifier):
            components["tables"].append({
                "name": token.get_real_name(),
                "alias": token.get_alias()
            })

    return components



def validate_tables(parsed_query, schema_info):
    """
    Validate table names in the SQL query against the schema.

    Args:
        parsed_query (dict): Parsed query components.
        schema_info (dict): Schema information.

    Returns:
        dict: Mapping of fully qualified table names -> schema details.

    Raises:
        ValueError: If a table is not found in the schema.
    """
    table_map = {}
    schema_tables = schema_info["tables"]

    for table in parsed_query["tables"]:
        table_name = table["name"]
        alias = table["alias"]

        # Match table name to fully qualified schema tables
        qualified_table_name = next(
            (schema_table for schema_table in schema_tables if schema_table.endswith(f".{table_name}")), None
        )

        if not qualified_table_name:
            raise ValueError(f"Table '{table_name}' not found in schema.")

        table_map[alias or qualified_table_name] = schema_tables[qualified_table_name]

    return table_map







def extract_sql_query(output_text):
    """
    Extract only the SQL query from the model's output.
    
    Args:
        output_text (str): Full output text from the model.

    Returns:
        str: Isolated SQL query.
    """
    try:
        # Look for "SQL Query:" marker and extract the part after it
        if "SQL Query:" in output_text:
            sql_query = output_text.split("SQL Query:")[-1].strip()
            return sql_query
        else:
            raise ValueError("SQL Query marker not found in the model output.")
    except Exception as e:
        raise ValueError(f"Error extracting SQL query: {e}")
    

def correct_sql_query(sql_query, table_map, corrected_columns):
    """
    Correct the SQL query using validated tables and columns.

    Args:
        sql_query (str): Original SQL query.
        table_map (dict): Validated alias-to-table mapping (alias -> fully qualified table name).
        corrected_columns (dict): Mapping of original column -> corrected column.

    Returns:
        str: Corrected SQL query.
    """
    # Correct table aliases with fully qualified table names
    for alias, full_table_name in table_map.items():
        # Replace the alias with its fully qualified table name
        if alias and isinstance(full_table_name, str):
            sql_query = sql_query.replace(f"{alias}.", f"{full_table_name}.")

    # Correct column names
    for original, corrected in corrected_columns.items():
        sql_query = sql_query.replace(original, corrected)

    return sql_query




def process_query_pipeline(sql_query, schema_info):
    """
    Process a SQL query to validate and correct against schema.

    Args:
        sql_query (str): SQL query to process.
        schema_info (dict): Schema information.

    Returns:
        str: Corrected SQL query or error message.
    """
    try:
        # Step 1: Parse SQL query
        parsed_query = parse_sql_query(sql_query)
        print("Parsed Query:", parsed_query)

        # Step 2: Validate tables
        table_map = validate_tables(parsed_query, schema_info)
        print("Validated Tables:", table_map)

        # Step 3: Validate and correct columns
        corrected_columns = validate_columns(parsed_query, table_map)
        print("Corrected Columns:", corrected_columns)

        # Step 4: Correct the SQL query
        corrected_query = correct_sql_query(sql_query, table_map, corrected_columns)
        return corrected_query

    except Exception as e:
        return f"Error: {str(e)}"


def execute_sql_query(sql_query):
    """
    Validate, correct, and execute the SQL query.
    """
    try:
        

        with connection.cursor() as cursor:
            cursor.execute(sql_query)
            columns = [col[0] for col in cursor.description]  # Get column names
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logger.info("SQL query executed successfully")
        return results

    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        return {"error": str(e)}