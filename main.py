import logging
import asyncio
from typing import List, Dict
from podcast_collector import collect_transcripts
from payload_builder import build_payload
from notifier import send_payload

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
)

def display_transcript_summary(results: List[Dict]) -> None:
    """
    Display a summary of the collected transcripts.
    
    Args:
        results (List[Dict]): List of transcript results with metadata
    """
    logging.info("\n=== Transcript Collection Summary ===")
    for i, result in enumerate(results, 1):
        metadata = result['metadata']
        logging.info(f"\nTranscript {i}:")
        logging.info(f"Title: {metadata.get('title', 'Unknown')}")
        logging.info(f"Duration: {metadata.get('duration', 0):.2f} seconds")
        logging.info(f"Word Count: {metadata.get('words', 0)}")

async def main() -> None:
    """
    Main entry point for the Podcast Transcript App.
    """
    try:
        logging.info('Starting Podcast Transcript App')
        
        # Example feeds (replace with actual podcast RSS feeds)
        feeds = [
            'https://feeds.npr.org/510318/podcast.xml',  # Example: NPR's Up First
            'https://feeds.megaphone.fm/WWO6655869236',  # Example: Prof G
        ]
        
        # Collect transcripts
        logging.info(f"Starting transcript collection for {len(feeds)} feeds...")
        results = await collect_transcripts(feeds)
        
        if not results:
            logging.error("No transcripts were collected!")
            return
            
        # Display summary of collected transcripts
        display_transcript_summary(results)
        
        # Extract just the transcripts for the payload
        transcripts = [result['transcript'] for result in results]
        
        # Build payload
        logging.info("Building payload...")
        payload = build_payload(transcripts)
        
        # Send to NotebookLLM (replace with actual endpoint)
        endpoint = "https://api.notebookllm.com/process"  # Replace with actual endpoint
        logging.info(f"Sending payload to {endpoint}...")
        send_payload(endpoint, payload)
        
        logging.info("Process completed successfully!")
        
    except Exception as e:
        logging.error(f"An error occurred in main: {str(e)}")
        raise

def run_main():
    """
    Entry point for the Poetry script.
    Handles running the async main function.
    """
    asyncio.run(main())

if __name__ == '__main__':
    run_main()
