# api_gateway/urls.py
from django.urls import path
from . import views
from .views import (
    connect_to_db,
    fetch_schemas,
    update_selected_schemas,
    process_query,
    validate_query,
    execute_query,
    inspect_session
)
urlpatterns = [
    path('query/', views.QueryView.as_view(), name='query'), 
    
    # Stage 1: Connect to Database
    path('connect/', connect_to_db, name='connect_to_db'),

    # Stage 2: Fetch Schemas
    path('schemas/', fetch_schemas, name='fetch_schemas'),

    # Stage 3: Update Selected Schemas
    path('schemas/update/', update_selected_schemas, name='update_selected_schemas'),

    # Stage 4: Process Query (Generate SQL)
    path('query/', process_query, name='process_query'),

    # Stage 5: Validate SQL Query
    path('query/validate/', validate_query, name='validate_query'),

    # Stage 6: Execute SQL Query
    path('query/execute/', execute_query, name='execute_query'),

    # Debugging: Inspect Session Data
    path('session/<str:session_id>/', inspect_session, name='inspect_session')



]
