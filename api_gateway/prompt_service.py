# api_gateway/prompt_service.py
import logging
from .utils import get_session_data
logger = logging.getLogger(__name__)



def generate_prompt(nl_query, mapped_schema):
    """
    Generate a stricter prompt for the model to ensure valid SQL generation.

    Args:
        nl_query (str): The user's natural language query.
        mapped_schema (dict): Schema information with fully qualified table names.

    Returns:
        str: Prompt for the model.
    """
    prompt = "Use the following database schema to generate a SQL query:\n"
    for table_name, table_info in mapped_schema["tables"].items():
        prompt += f"Table: {table_name}\n"
        prompt += "Columns (use only these):\n"
        for column_name, column_info in table_info["columns"].items():
            prompt += f"  - {column_name} ({column_info['data_type']})\n"
        prompt += "Relations:\n"
        for relation, details in table_info["relations"].items():
            prompt += f"  - {relation} -> {details['related_table']}({details['related_column']})\n"
        prompt += "\n"
    prompt += "Rules:\n"
    prompt += "1. Use only the column names listed above.\n"
    prompt += "2. Do not fabricate column names.\n"
    prompt += "3. Ensure valid SQL syntax.\n"
    prompt += f"User Query: {nl_query}\n"
    prompt += "SQL Query:"
    return prompt.strip()






def generate_followup_prompt(nl_query, relevant_schemas, session_id):
    # Retrieve schema context from memory for the session
    schema_context = get_session_data.get(session_id, "")
    schema_description = "\n".join(relevant_schemas)

    # Combine previous schema context with the new query
    prompt = f"""
    Database schema:
    {schema_context}
    {schema_description}

    User follow-up query: {nl_query}
    translate English to SQL based on context above:
    """
    return prompt


