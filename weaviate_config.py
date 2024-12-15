import os
from typing import Optional
import weaviate
from weaviate.auth import AuthApiKey
from weaviate.connect import ConnectionParams
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Schema definitions
CLASS_OBJ_EMAILS = {
    "class": "Email",
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "text2vec-openai": {
            "model": "ada",
            "modelVersion": "002",
            "type": "text"
        }
    },
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
            "dataType": ["TEXT"],
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
    "moduleConfig": {
        "text2vec-openai": {
            "model": "ada",
            "modelVersion": "002",
            "type": "text"
        }
    },
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
            "dataType": ["TEXT"],
            "description": "Name of the podcast show",
        },
        {
            "name": "duration",
            "dataType": ["number"],
            "description": "Duration in seconds",
        },
        {
            "name": "episodeUrl",
            "dataType": ["TEXT"],
        },
        {
            "name": "feedUrl",
            "dataType": ["TEXT"],
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

    client = weaviate.WeaviateClient(
        connection_params=ConnectionParams.from_params(
            http_host="localhost",     # Localhost for HTTP
            http_port=8080,           # Default HTTP port
            http_secure=False,        # False since you're using HTTP (not HTTPS)
            grpc_host="localhost",    # Localhost for gRPC
            grpc_port=50051,          # Default gRPC port
            grpc_secure=False         # False since you're not using secure gRPC
        )
    )
    
    # Explicitly connect the client
    client.connect()
    if not client.is_ready():
        raise RuntimeError("Failed to connect to the Weaviate instance.")
    
    return client


def ensure_schema_exists(client: weaviate.WeaviateClient) -> None:
    """
    Ensure that the required collections exist in Weaviate.
    Args:
        client (weaviate.WeaviateClient): Initialized Weaviate client
    """
    try:
        # List all existing collections
        existing_collections = client.collections.list_all()

        # Check and create Email collection if it doesn't exist
        if "Email" not in existing_collections:
            client.collections.create(
                name="Email",
                vectorizer_config=weaviate.classes.config.Configure.Vectorizer.text2vec_openai(),
                properties=[
                    weaviate.classes.config.Property(
                        name="content",
                        data_type=weaviate.classes.config.DataType.TEXT,
                        description="The email content",
                        vectorize_property_name=True,
                    ),
                    weaviate.classes.config.Property(
                        name="subject",
                        data_type=weaviate.classes.config.DataType.TEXT,
                        description="Email subject",
                    ),
                    weaviate.classes.config.Property(
                        name="sender",
                        data_type=weaviate.classes.config.DataType.TEXT,
                    ),
                    weaviate.classes.config.Property(
                        name="timestamp",
                        data_type=weaviate.classes.config.DataType.DATE,
                    ),
                ]
            )

        # Check and create PodcastTranscript collection if it doesn't exist
        if "PodcastTranscript" not in existing_collections:
            client.collections.create(
                name="PodcastTranscript",
                vectorizer_config=weaviate.classes.config.Configure.Vectorizer.text2vec_openai(),
                properties=[
                    weaviate.classes.config.Property(
                        name="transcript",
                        data_type=weaviate.classes.config.DataType.TEXT,
                        description="The podcast transcript",
                        vectorize_property_name=True,
                    ),
                    weaviate.classes.config.Property(
                        name="title",
                        data_type=weaviate.classes.config.DataType.TEXT,
                        description="Episode title",
                    ),
                    weaviate.classes.config.Property(
                        name="podcastName",
                        data_type=weaviate.classes.config.DataType.TEXT,
                        description="Name of the podcast show",
                    ),
                    weaviate.classes.config.Property(
                        name="duration",
                        data_type=weaviate.classes.config.DataType.NUMBER,
                        description="Duration in seconds",
                    ),
                    weaviate.classes.config.Property(
                        name="episodeUrl",
                        data_type=weaviate.classes.config.DataType.TEXT,
                    ),
                    weaviate.classes.config.Property(
                        name="feedUrl",
                        data_type=weaviate.classes.config.DataType.TEXT,
                    ),
                    weaviate.classes.config.Property(
                        name="wordCount",
                        data_type=weaviate.classes.config.DataType.NUMBER,
                    ),
                    weaviate.classes.config.Property(
                        name="publishedDate",
                        data_type=weaviate.classes.config.DataType.DATE,
                    ),
                ]
            )

    except Exception as e:
        print(f"Error ensuring collection existence: {e}")
        raise



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
            "podcastName": transcript_data["metadata"].get(
                "podcast_name",
                transcript_data["metadata"]["feed_url"].split("/")[-1]
            ),
            "publishedDate": transcript_data["metadata"].get("published_date")
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