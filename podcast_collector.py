import logging
import asyncio
import os
from typing import List, Tuple, Dict
from dotenv import load_dotenv
from deepgram import (
    DeepgramClient,
    PrerecordedOptions,
    FileSource
)
import feedparser

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
)

# Load environment variables
load_dotenv()
DEEPGRAM_API_KEY = os.getenv('DEEPGRAM_API_KEY')

async def transcribe_url(url: str) -> Tuple[str, Dict]:
    """
    Transcribe a podcast episode from a URL using Deepgram.

    Args:
        url (str): URL of the podcast episode

    Returns:
        Tuple[str, Dict]: Tuple containing (transcript text, metadata)
    """
    try:
        logging.info(f"Starting transcription for URL: {url}")
        
        # Initialize Deepgram
        deepgram = DeepgramClient(DEEPGRAM_API_KEY)
        
        # Configure transcription options
        options = PrerecordedOptions(
            smart_format=True,
            punctuate=True,
            diarize=True,
            paragraphs=True
        )
        
        logging.info("Sending request to Deepgram...")
        source = FileSource(url=url)
        response = await deepgram.listen.prerecorded.v("1").transcribe_url(source, options)
        
        # Get the transcript and metadata from the response
        results = response.results
        transcript = results.channels[0].alternatives[0].paragraphs.transcript
        
        # Extract metadata
        metadata = {
            'duration': results.metadata.duration,
            'channels': results.metadata.channels,
            'words': len(results.channels[0].alternatives[0].words)
        }
        
        # Log first 10 lines of transcript
        if transcript:
            preview_lines = transcript.split('\n')[:10]
            logging.info("Transcript preview (first 10 lines):")
            for line in preview_lines:
                logging.info(f"    {line}")
                
            logging.info(f"Transcription completed. Duration: {metadata['duration']}s, Words: {metadata['words']}")
        else:
            logging.warning("No transcript was generated")
            
        return transcript, metadata
        
    except Exception as e:
        logging.error(f"Error transcribing URL {url}: {str(e)}")
        return "", {}

async def process_feed(feed_url: str) -> List[Dict]:
    """
    Process a podcast feed and transcribe its latest episode.

    Args:
        feed_url (str): URL of the podcast RSS feed

    Returns:
        List[Dict]: List containing transcripts and their metadata
    """
    try:
        if not DEEPGRAM_API_KEY:
            raise ValueError("DEEPGRAM_API_KEY not found in environment variables")
            
        logging.info(f"Processing feed: {feed_url}")
        
        logging.info("Fetching RSS feed...")
        rss = feedparser.parse(feed_url)
        
        if not rss.entries:
            logging.warning(f"No entries found in feed: {feed_url}")
            return []
            
        episode = rss.entries[0]
        episode_url = episode.enclosures[0].href
        episode_title = episode.title
        
        logging.info(f"Found latest episode: {episode_title}")
        logging.info(f"Episode URL: {episode_url}")
        
        transcript, metadata = await transcribe_url(episode_url)
        
        if transcript:
            result = {
                'transcript': transcript,
                'metadata': {
                    **metadata,
                    'title': episode_title,
                    'feed_url': feed_url,
                    'episode_url': episode_url
                }
            }
            return [result]
        return []
        
    except Exception as e:
        logging.error(f"Error processing feed {feed_url}: {str(e)}")
        return []

async def collect_transcripts(feeds: List[str]) -> List[Dict]:
    """
    Collects transcripts from the provided podcast feeds.

    Args:
        feeds (List[str]): A list of podcast feed URLs.

    Returns:
        List[Dict]: A list of transcripts and their metadata
    """
    all_results = []
    total_feeds = len(feeds)
    
    logging.info(f"Starting transcript collection for {total_feeds} feeds")
    
    for index, feed in enumerate(feeds, 1):
        logging.info(f"Processing feed {index}/{total_feeds}: {feed}")
        feed_results = await process_feed(feed)
        all_results.extend(feed_results)
        
        logging.info(f"Completed feed {index}/{total_feeds}")
        logging.info(f"Current progress: {index/total_feeds*100:.1f}%")
    
    logging.info(f"Transcript collection completed. Total transcripts: {len(all_results)}")
    return all_results
