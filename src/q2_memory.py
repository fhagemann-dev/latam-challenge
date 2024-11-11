import json
from typing import List, Tuple
from collections import Counter
import emoji
import logging

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q2_memory")

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Extracts and counts the top 10 most used emojis from a JSON file containing tweets.
    Memory-optimized function.
    
    Parameters
    ----------
    file_path : str
        Path to the JSON file.

    Returns
    -------
    list
        A list of tuples with the top 10 emojis and their counts.
    """
    try:
        # Initialize a Counter to keep track of emojis
        emoji_counts = Counter()
        
        # Define the set of emojis from emoji.EMOJI_DATA
        emoji_set = set(emoji.EMOJI_DATA.keys())

        with open(file_path, 'r') as file:
            for line in file:
                tweet = json.loads(line)
                content = tweet.get('content', '')
                emojis = [char for char in content if char in emoji_set]
                

                emoji_counts.update(emojis)

        top_10_emojis = emoji_counts.most_common(10)
        logger.info("Top 10 emojis with their counts calculated successfully.")
        
        return top_10_emojis

    except Exception as e:
        logger.error(f"An error occurred while processing the emojis: {e}")
        return []