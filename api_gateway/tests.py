# api_gateway/tests.py
import pytest
from django.urls import reverse
from rest_framework.test import APIClient
from .embedding_service import retrieve_relevant_schema, store_schema_embeddings
from .prompt_service import generate_prompt, generate_followup_prompt
from .sql_service import validate_sql, execute_sql_query
from .schema_extractor import get_schema
from django.conf import settings

client = APIClient()

@pytest.mark.django_db
def test_retrieve_relevant_schema():
    # Example natural language query
    query = "Count of employees born after 1970"
    store_schema_embeddings()
    result = retrieve_relevant_schema(query)
    assert len(result) > 0, "No relevant schemas found"

@pytest.mark.django_db
def test_generate_prompt():
    nl_query = "Show total sales for each customer"
    relevant_schemas = ["public.customers", "public.sales"]
    prompt = generate_prompt(nl_query, relevant_schemas)
    assert "public.customers" in prompt and "public.sales" in prompt, "Prompt missing relevant schemas"

@pytest.mark.django_db
def test_sql_validation():
    # Example SQL query (assuming a simple schema with `public.customers`)
    sql_query = "SELECT * FROM public.customers"
    schema_info = get_schema()
    is_valid = validate_sql(sql_query, schema_info)
    assert is_valid, "SQL validation failed"

@pytest.mark.django_db
def test_execute_sql_query():
    # Execute a simple, valid SQL query
    sql_query = "SELECT 1 AS test_column"
    result = execute_sql_query(sql_query)
    assert result == [{"test_column": 1}], "SQL execution failed or returned incorrect result"

@pytest.mark.django_db
def test_query_view_initial_query():
    # Test the entire flow from query input to SQL execution
    response = client.post(reverse('query'), {'query': "Show all employees with departments"})
    assert response.status_code == 200, "QueryView failed with status code"
    assert "sql_query" in response.data, "SQL query not generated"
    assert "results" in response.data, "SQL results not returned"
