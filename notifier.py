import logging
import requests
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_payload(endpoint: str, payload: Dict[str, Any]) -> None:
    """
    Sends the payload to the specified endpoint.

    Args:
        endpoint (str): The URL of the endpoint to send the payload to.
        payload (Dict[str, Any]): The payload to send.
    """
    logging.info(f'Sending payload to {endpoint}')
    response = requests.post(endpoint, json=payload)
    logging.info(f'Response status: {response.status_code}, Response: {response.json()}')
