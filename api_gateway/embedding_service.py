# api_gateway/embedding_service.py
from sentence_transformers import SentenceTransformer
from .utils import initialize_pinecone
from .schema_extractor import get_schema, flatten_schema_info
import logging

logger = logging.getLogger(__name__)

# Initialize the Sentence Transformer model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
# Initialize Pinecone index
pinecone_index = initialize_pinecone()

def store_schema_embeddings():
    """
    Generate and store schema embeddings in Pinecone. 
    Break the process and raise an error on any failure.
    """
    schema_info = get_schema()  # Retrieve the schema information from the database
    schema_elements = flatten_schema_info(schema_info)
    
    for element in schema_elements:
        print("\n Schema element:", element)
        
        # Generate embedding for the schema element description
        description = element.get("description", "")
        if not description:
            raise ValueError(f"Missing description for schema element: {element}")
        
        embedding = model.encode(description).tolist()

        # Prepare metadata for Pinecone
        metadata = {
            "type": element.get("type", "unknown"),
            "schema": element.get("schema", "unknown"),
            "table": element.get("table", "unknown"),
            "name": element.get("name"),
            "description": description
        }
        
        if element["type"] == "foreign_key":
            metadata.update({
                "related_table": element.get("related_table", "unknown"),
                "related_column": element.get("related_column", "unknown"),
                "constraint_name": element.get("constraint_name", "unknown")
            })
        
        # Special handling for primary_key type
        if element["type"] == "primary_key":
            metadata["name"] = element.get("column", "unknown")
        
        # Validate metadata to ensure no invalid values
        for key, value in metadata.items():
            if value is None:
                raise ValueError(f"Invalid metadata for element: {element}. Missing value for '{key}'.")

        # Attempt to store embedding in Pinecone
        try:
            pinecone_index.upsert([(description, embedding, metadata)])
        except Exception as e:
            raise RuntimeError(f"Error storing embedding for element: {description}. Error: {e}")



def retrieve_relevant_schema(query):
    # Step 1: Generate embedding for the query
    query_embedding = model.encode([query])[0]
    
    # Step 2: Retrieve top relevant schema elements from Pinecone
    try:
        result = pinecone_index.query(
            vector=query_embedding.tolist(),
            top_k=5,  # Retrieve top 5 relevant schema elements
            include_metadata=True
        )
        
        return [
            {
                "name": match["metadata"].get("name"),
                "type": match["metadata"].get("type"),
                "schema": match["metadata"].get("schema"),
                "description": match["metadata"]["description"],
                "score": match["score"]
            }
            for match in result["matches"]
        ]

    except Exception as e:
        logger.error(f"Error retrieving relevant schema: {e}")
        return []
    

#store_schema_embeddings()