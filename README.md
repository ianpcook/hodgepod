# Podcast Transcript App

This application collects podcast transcripts from various feeds, consolidates them into a single payload, and sends them to NotebookLLM for processing.

## Installation

1. Install Poetry (if you haven't already):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. Install dependencies:
   ```bash
   poetry install
   ```

## Usage

Run the application using Poetry:
```bash
poetry run python main.py
```

Adjust the feeds in `podcast_collector.py` as needed.
