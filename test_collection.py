import asyncio
import logging
from podcast_collector import collect_transcripts

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

async def main():
    # Test feeds
    test_feeds = [
        'https://feeds.npr.org/510318/podcast.xml',  # NPR's Up First
        'https://feeds.megaphone.fm/WWO6655869236'   # Prof G
    ]
    
    for feed in test_feeds:
        await test_single_feed(feed)
        logging.info("\n" + "="*50 + "\n")  # Separator between feeds

if __name__ == '__main__':
    asyncio.run(main())
