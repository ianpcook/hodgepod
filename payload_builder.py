from typing import List, Dict


def build_payload(transcripts: List[str]) -> Dict[str, List[str]]:
    """
    Builds a payload from the collected transcripts.

    Args:
        transcripts (List[str]): A list of transcripts.

    Returns:
        Dict[str, List[str]]: A dictionary containing the transcripts.
    """
    return {'transcripts': transcripts}
