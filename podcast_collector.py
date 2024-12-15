import asyncio
import logging
from typing import Dict, List, Optional
import feedparser
from deepgram import Deepgram
import aiohttp
import os
from datetime import datetime
from dotenv import load_dotenv
from weaviate_config import init_weaviate_client, ensure_schema_exists, store_podcast_transcript

# Load environment variables
load_dotenv()

async def transcribe_audio(audio_url: str, dg_client: Deepgram) -> Optional[Dict]:
    """
    Transcribe audio from URL using Deepgram.
    """
    try:
        source = {'url': audio_url}
        response = await dg_client.listen.prerecorded.v("1").transcribe_url(source)
        
        if response and 'results' in response:
            return {
                'transcript': response['results']['channels'][0]['alternatives'][0]['transcript'],
                'words': len(response['results']['channels'][0]['alternatives'][0]['transcript'].split())
            }
        return None
        
    except Exception as e:
        logging.error(f"Error transcribing audio: {str(e)}")
        return None

async def process_feed(feed_url: str, dg_client: Deepgram) -> List[Dict]:
    """
    Process a podcast RSS feed and transcribe episodes.
    """
    try:
        feed = feedparser.parse(feed_url)
        transcripts = []
        
        for entry in feed.entries[:1]:  # Process only the latest episode for testing
            audio_url = None
            duration = None
            
            # Find the audio URL and duration
            for link in entry.links:
                if link.type and 'audio' in link.type:
                    audio_url = link.href
                    duration = float(getattr(entry, 'itunes_duration', 0))
                    break
            
            if audio_url:
                transcript_data = await transcribe_audio(audio_url, dg_client)
                
                if transcript_data:
                    transcript_data['metadata'] = {
                        'title': entry.title,
                        'duration': duration,
                        'episode_url': audio_url,
                        'feed_url': feed_url,
                        'published_date': datetime(*entry.published_parsed[:6]).isoformat(),
                        'words': transcript_data['words']
                    }
                    transcripts.append(transcript_data)
                    
        return transcripts
        
    except Exception as e:
        logging.error(f"Error processing feed: {str(e)}")
        return []

async def collect_transcripts(feed_urls: List[str]) -> None:
    """
    Collect and store transcripts from multiple podcast feeds.
    """
    try:
        # Initialize clients
        dg_client = Deepgram(os.getenv('DEEPGRAM_API_KEY'))
        weaviate_client = init_weaviate_client()
        
        # Ensure Weaviate schema exists
        ensure_schema_exists(weaviate_client)
        
        # Process each feed
        for feed_url in feed_urls:
            transcripts = await process_feed(feed_url, dg_client)
            
            # Store transcripts in Weaviate
            for transcript in transcripts:
                uuid = store_podcast_transcript(weaviate_client, transcript)
                if uuid:
                    logging.info(f"Stored transcript with UUID: {uuid}")
                else:
                    logging.error("Failed to store transcript")
                    
    except Exception as e:
        logging.error(f"Error in collect_transcripts: {str(e)}")
    finally:
        await dg_client.close()

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Example feed URLs
    feed_urls = [
        "https://feeds.megaphone.fm/huberman",
        "https://lexfridman.com/feed/podcast/"
    ]
    
    # Run the collector
    asyncio.run(collect_transcripts(feed_urls))
