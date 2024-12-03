from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from .schema_extractor import get_schema, map_relevant_schemas_to_tables,standardize_table_names
from .embedding_service import retrieve_relevant_schema
from .sql_service import  execute_sql_query
from .llm_service import generate_sql_from_nl
from .utils import save_session_data ,get_session_data,get_full_session
import psycopg2
import logging

logger = logging.getLogger(__name__)



class QueryView(APIView):
    def post(self, request):
        nl_query = request.data.get("query")
        session_id = request.data.get("session_id")

        if not nl_query or len(nl_query.strip()) < 5:
            return Response({"error": "A valid natural language query (at least 5 characters) is required."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Step 1: Retrieve schema information
            schema_info = get_schema()
            print(" \n Fetched SChema Info : ", type(schema_info))


            # Step 2: Retrieve relevant schemas
            relevant_schemas = retrieve_relevant_schema(nl_query)
            print(" \n Fetched  Relevant SChema : ", (relevant_schemas))


            # Step 3: Map relevant schemas to tables
            mapped_schema_info = map_relevant_schemas_to_tables(relevant_schemas, schema_info)
            print(" \n Mapped  SChema Info : ", mapped_schema_info)
            logger.info(f"Mapped schema info: {mapped_schema_info}")

            # Step 4: Generate SQL using SQLCoder
            sql_query = generate_sql_from_nl(nl_query, mapped_schema_info)
            
            
            print(f" \n Generated SQL Query before STD \n: {sql_query} ")
            logger.info(f"Generated SQL Query: {sql_query}")

            std_query = standardize_table_names(sql_query,mapped_schema_info)
            print(f" \n Generated SQL Query After STD \n: {std_query} ")

            if not sql_query:
                return Response({"error": "Failed to generate SQL query."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Step 5: Validate SQL query (Optional: Uncomment if required)
            
            #corrected_query = process_query_pipeline(sql_query, mapped_schema_info)
            

            # Step 6: Execute SQL query
            results = execute_sql_query(std_query)
            """if "error" in results:
                return Response({"error": results["error"]}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)"""

            # Save session details
            save_session_data(session_id, "last_query", nl_query)
            save_session_data(session_id, "last_sql", sql_query)

            return Response({"sql_query": sql_query, "results": results}, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return Response({f"error": "An error occurred while processing the query. {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



@api_view(['POST'])
def connect_to_db(request):
    session_id = request.data.get("session_id")
    db_credentials = {
        "db_host": request.data.get("host"),
        "db_port": request.data.get("port"),
        "db_name": request.data.get("database"),
        "db_user": request.data.get("user"),
        "db_password": request.data.get("password")
    }

    try:
        # Test the connection using psycopg2
        conn = psycopg2.connect(**db_credentials)
        conn.close()

        # Save DB credentials to Redis
        save_session_data(session_id, "db_credentials", db_credentials)

        # Update pipeline stage
        save_session_data(session_id, "pipeline_stage", "db_connected")

        return Response({"message": "Database connected successfully."}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": f"Database connection failed: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



@api_view(['GET'])
def fetch_schemas(request):
    session_id = request.data.get("session_id")

    try:
        # Retrieve DB credentials from Redis
        db_credentials = get_session_data(session_id, "db_credentials")

        # Fetch schemas from the database
        schema_info = get_schema(db_credentials)

        # Save schemas to Redis
        save_session_data(session_id, "available_schemas", schema_info)

        # Update pipeline stage
        save_session_data(session_id, "pipeline_stage", "schemas_discovered")

        return Response({"schemas": schema_info}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": f"Failed to fetch schemas: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@api_view(['POST'])
def update_selected_schemas(request):
    session_id = request.data.get("session_id")
    selected_schemas = request.data.get("schemas")

    try:
        # Validate selected schemas against available schemas
        available_schemas = get_session_data(session_id, "available_schemas")
        invalid_schemas = [schema for schema in selected_schemas if schema not in available_schemas]
        if invalid_schemas:
            return Response({"error": f"Invalid schemas: {invalid_schemas}"}, status=status.HTTP_400_BAD_REQUEST)

        # Save selected schemas to Redis
        save_session_data(session_id, "selected_schemas", selected_schemas)

        # Update pipeline stage
        save_session_data(session_id, "pipeline_stage", "schemas_selected")

        return Response({"message": "Schemas updated successfully."}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": f"Failed to update schemas: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    

@api_view(['POST'])
def process_query(request):
    session_id = request.data.get("session_id")
    nl_query = request.data.get("query")

    try:
        # Retrieve schemas and schema info from Redis
        selected_schemas = get_session_data(session_id, "selected_schemas")

        # Generate SQL query using the LLM
        sql_query = generate_sql_from_nl(nl_query, selected_schemas)

        # Save the NL query and generated SQL to Redis
        save_session_data(session_id, "nl_query", nl_query)
        save_session_data(session_id, "sql_query", sql_query)

        # Update pipeline stage
        save_session_data(session_id, "pipeline_stage", "query_processed")

        return Response({"sql_query": sql_query}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": f"Failed to process query: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
def validate_query(request):
    session_id = request.data.get("session_id")

    try:
        # Retrieve SQL query from Redis
        sql_query = get_session_data(session_id, "sql_query")

        # Validate SQL
        #is_valid = validate_sql(sql_query)
        if not is_valid:
            return Response({"error": "Generated SQL query is invalid or potentially unsafe."}, status=status.HTTP_400_BAD_REQUEST)

        # Update pipeline stage
        save_session_data(session_id, "pipeline_stage", "query_validated")

        return Response({"message": "SQL query validated successfully."}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": f"Failed to validate query: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
def execute_query(request):
    session_id = request.data.get("session_id")

    try:
        # Retrieve SQL query and DB credentials from Redis
        sql_query = get_session_data(session_id, "sql_query")
        db_credentials = get_session_data(session_id, "db_credentials")

        # Execute the SQL query
        results = execute_sql_query(sql_query, db_credentials)

        # Save results to Redis
        save_session_data(session_id, "query_results", results)

        # Update pipeline stage
        save_session_data(session_id, "pipeline_stage", "query_executed")

        return Response({"results": results}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": f"Failed to execute query: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@api_view(['GET'])
def inspect_session(request, session_id):
    try:
        session_data = get_full_session(session_id)
        return Response({"session_data": session_data}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": f"Failed to inspect session: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


