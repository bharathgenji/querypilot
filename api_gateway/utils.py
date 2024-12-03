# api_gateway/utils.py
import os
from pinecone import Pinecone, ServerlessSpec

from redis import Redis
import json

# Initialize Redis client
redis_client = Redis(host="localhost", port=6379, decode_responses=True)

def save_session_data(session_id, key, value):
    """
    Save a key-value pair to the session in Redis.
    """
    redis_client.hset(f"session:{session_id}", key, json.dumps(value))

def get_session_data(session_id, key):
    """
    Retrieve a value by key from the session in Redis.
    """
    data = redis_client.hget(f"session:{session_id}", key)
    return json.loads(data) if data else None

def get_full_session(session_id):
    """
    Retrieve all session data for a given session ID.
    """
    data = redis_client.hgetall(f"session:{session_id}")
    return {key: json.loads(value) for key, value in data.items()}

def delete_session(session_id):
    """
    Delete a session from Redis.
    """
    redis_client.delete(f"session:{session_id}")

def update_pipeline_stage(session_id, stage):
    save_session_data(session_id, "pipeline_stage", stage)


def initialize_pinecone():
    # Create a Pinecone instance
    pc = Pinecone(
        api_key=os.getenv('PINECONE_API_KEY')
    )

    index_name = "schema-index"

    # Check if the index exists, otherwise create it
    if index_name not in [index.name for index in pc.list_indexes().indexes]:
        pc.create_index(
            name=index_name,
            dimension=384,  # Adjust the dimension to match the Sentence Transformers embedding model
            metric="cosine",  # Choose an appropriate similarity metric
            spec=ServerlessSpec(
                cloud="aws",
                region="us-east-1"
            )
        )
    
    return pc.Index(index_name)
