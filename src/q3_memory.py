import json
from typing import List, Tuple
from collections import Counter
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q3_memory")

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Extracts and counts the top 10 most mentioned usernames from a JSON file containing tweets.
    Memory-optimized function.
    
    Parameters
    ----------
    file_path : str
        Path to the JSON file.

    Returns
    -------
    list
        A list of tuples with the top 10 usernames and their mention counts.
    """
    try:
        # Initialize a Counter to keep track of mention counts
        mentions_counts = Counter()
        
        # Read the file line by line
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    tweet = json.loads(line)
                    mentioned_users = tweet.get('mentionedUsers')
                    
                    # Count mentions if any mentioned users are present
                    if mentioned_users:
                        for user_info in mentioned_users:
                            user = user_info.get('username')
                            if user:
                                mentions_counts[user] += 1
                                
                except json.JSONDecodeError:
                    logger.warning("Skipped a line due to JSON decoding error.")
        
        # Get the top 10 most mentioned users
        top_10_users = mentions_counts.most_common(10)
        logger.info("Top 10 most mentioned users calculated successfully.")
        
        return top_10_users
    
    except Exception as e:
        logger.error(f"An error occurred while processing the mentions: {e}")
        return []