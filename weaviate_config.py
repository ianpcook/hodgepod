import os
from typing import Optional
import weaviate
from dotenv import load_dotenv
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey

# Load environment variables
load_dotenv()

# Schema definitions
CLASS_OBJ_EMAILS = {
    "class": "Email",
    "vectorizer": "text2vec-openai",
    "properties": [
        {
            "name": "content",
            "dataType": ["text"],
            "description": "The email content",
            "vectorizePropertyName": True,
        },
        {
            "name": "subject",
            "dataType": ["text"],
            "description": "Email subject",
        },
        {
            "name": "sender",
            "dataType": ["string"],
        },
        {
            "name": "timestamp",
            "dataType": ["date"],
        }
    ]
}

CLASS_OBJ_PODCASTS = {
    "class": "PodcastTranscript",
    "vectorizer": "text2vec-openai",
    "properties": [
        {
            "name": "transcript",
            "dataType": ["text"],
            "description": "The podcast transcript",
            "vectorizePropertyName": True,
        },
        {
            "name": "title",
            "dataType": ["text"],
            "description": "Episode title",
        },
        {
            "name": "podcastName",
            "dataType": ["string"],
            "description": "Name of the podcast show",
        },
        {
            "name": "duration",
            "dataType": ["number"],
            "description": "Duration in seconds",
        },
        {
            "name": "episodeUrl",
            "dataType": ["string"],
        },
        {
            "name": "feedUrl",
            "dataType": ["string"],
        },
        {
            "name": "wordCount",
            "dataType": ["number"],
        },
        {
            "name": "publishedDate",
            "dataType": ["date"],
        }
    ]
}

def init_weaviate_client() -> weaviate.WeaviateClient:
    """
    Initialize the Weaviate client with configuration.
    Returns:
        weaviate.WeaviateClient: Configured Weaviate client instance
    """
    # Get configuration from environment variables
    WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")
    WEAVIATE_GRPC_PORT = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    # Create connection parameters
    connection_params = ConnectionParams.from_url(
        url=WEAVIATE_URL,
        grpc_port=WEAVIATE_GRPC_PORT
    )
    
    # Create auth credentials
    auth = AuthApiKey(api_key=OPENAI_API_KEY)
    
    # Initialize client with v4 syntax
    client = weaviate.WeaviateClient(
        connection_params=connection_params,
        auth_credentials=auth
    )
    
    return client

def ensure_schema_exists(client: weaviate.WeaviateClient) -> None:
    """
    Ensure that the required schema classes exist in Weaviate.
    Args:
        client (weaviate.WeaviateClient): Initialized Weaviate client
    """
    # Check and create Email class if it doesn't exist
    if not client.schema.exists("Email"):
        client.schema.create_class(CLASS_OBJ_EMAILS)
    
    # Check and create PodcastTranscript class if it doesn't exist
    if not client.schema.exists("PodcastTranscript"):
        client.schema.create_class(CLASS_OBJ_PODCASTS)

def store_podcast_transcript(client: weaviate.WeaviateClient, transcript_data: dict) -> Optional[str]:
    """
    Store a podcast transcript in Weaviate.
    Args:
        client (weaviate.WeaviateClient): Initialized Weaviate client
        transcript_data (dict): Dictionary containing transcript and metadata
    Returns:
        Optional[str]: UUID of the created object if successful, None otherwise
    """
    try:
        # Prepare the data object
        data_object = {
            "transcript": transcript_data["transcript"],
            "title": transcript_data["metadata"]["title"],
            "duration": transcript_data["metadata"]["duration"],
            "wordCount": transcript_data["metadata"]["words"],
            "episodeUrl": transcript_data["metadata"]["episode_url"],
            "feedUrl": transcript_data["metadata"]["feed_url"],
            "podcastName": transcript_data["metadata"].get("podcast_name", 
                         transcript_data["metadata"]["feed_url"].split("/")[-1])
        }
        
        # Store in Weaviate
        result = client.data_object.create(
            class_name="PodcastTranscript",
            data_object=data_object
        )
        
        return result["id"]
    
    except Exception as e:
        print(f"Error storing transcript in Weaviate: {str(e)}")
        return None
