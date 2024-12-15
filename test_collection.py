import asyncio
import logging
import unittest
from podcast_collector import collect_transcripts
from weaviate_config import init_weaviate_client, ensure_schema_exists, CLASS_OBJ_PODCASTS
import weaviate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
)

async def test_single_feed(feed_url: str) -> None:
    """
    Test transcript collection for a single feed.
    
    Args:
        feed_url (str): URL of the podcast RSS feed to test
    """
    logging.info(f"\n=== Testing Transcript Collection for Feed ===\n{feed_url}")
    
    try:
        results = await collect_transcripts([feed_url])
        
        if not results:
            logging.error("No transcripts were collected!")
            return
            
        # Display detailed results
        for result in results:
            metadata = result['metadata']
            logging.info("\n=== Transcript Details ===")
            logging.info(f"Title: {metadata['title']}")
            logging.info(f"Duration: {metadata['duration']:.2f} seconds")
            logging.info(f"Word Count: {metadata['words']}")
            
            # Show first 100 words of transcript
            words = result['transcript'].split()[:100]
            preview = ' '.join(words)
            logging.info("\n=== Transcript Preview (first 100 words) ===")
            logging.info(preview)
            
    except Exception as e:
        logging.error(f"Error during testing: {str(e)}")
        raise

class TestWeaviateConnection(unittest.TestCase):
    def setUp(self):
        self.client = init_weaviate_client()
        
    def test_weaviate_connection(self):
        """Test if Weaviate is running and accessible"""
        self.assertTrue(self.client.is_live())
        
class TestSchemaSetup(unittest.TestCase):
    def setUp(self):
        self.client = init_weaviate_client()
        
    def test_schema_creation(self):
        """Test if schema classes can be created"""
        ensure_schema_exists(self.client)
        
        # Verify PodcastTranscript class exists
        self.assertTrue(
            self.client.schema.exists("PodcastTranscript"),
            "PodcastTranscript class should exist after schema creation"
        )
        
    def test_schema_properties(self):
        """Test if schema properties are correctly set"""
        ensure_schema_exists(self.client)
        
        # Get schema for PodcastTranscript
        schema = self.client.schema.get("PodcastTranscript")
        
        # Check required properties exist
        properties = {prop["name"] for prop in schema["properties"]}
        required_props = {"transcript", "title", "duration", "podcastName"}
        
        for prop in required_props:
            self.assertIn(
                prop,
                properties,
                f"Property {prop} should exist in PodcastTranscript schema"
            )

async def main():
    # First test Weaviate setup
    suite = unittest.TestSuite()
    suite.addTest(TestWeaviateConnection('test_weaviate_connection'))
    suite.addTest(TestSchemaSetup('test_schema_creation'))
    suite.addTest(TestSchemaSetup('test_schema_properties'))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        logging.error(" Weaviate setup failed, stopping tests")
        return
        
    # Test feeds
    test_feeds = [
        'https://feeds.npr.org/510318/podcast.xml',  # NPR's Up First
        'https://feeds.megaphone.fm/WWO6655869236'   # Prof G
    ]
    
    for feed in test_feeds:
        await test_single_feed(feed)
        logging.info("\n==================================================\n")

if __name__ == '__main__':
    asyncio.run(main())
