import logging
import json
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q1_memory")

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """ 
    Processes a JSON file to find the top 10 dates with the most tweets 
    and the user with the most posts for each date. Memory-optimized function.
    
    Parameters
    ----------
    file_path : str
        Path to the JSON file.

    Returns
    -------
    list
        A list of tuples with the top 10 dates and the username with the most published 
        tweets on each of those dates.
    """
    try:
        date_user_count = defaultdict(lambda: defaultdict(int))
        logger.info("Initialized date-user counter.")
        
        # Read file line by line and count tweets per date and username
        with open(file_path, 'r') as f:
            for line in f:
                tweet = json.loads(line)
                
                date_str = tweet.get("date", "").split("T")[0]
                username = tweet.get("user", {}).get("username")
                
                if date_str and username:
                    date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    date_user_count[date][username] += 1

        # Prepare list of (date, username, tweet_count) tuples for sorting
        date_user_list = [
            (date, username, count)
            for date, users in date_user_count.items()
            for username, count in users.items()
        ]

        # Sort by tweet count (descending) and username (ascending)
        date_user_list.sort(key=lambda x: (-x[2], x[1]))

        # Identify the most active user per date, keeping only the first occurrence per date
        top_by_date = {}
        for date, username, count in date_user_list:
            if date not in top_by_date:
                top_by_date[date] = (username, count)
        
        # Sort by tweet count to get the top 10 dates with the most tweets
        top_10_dates = sorted(top_by_date.items(), key=lambda x: -x[1][1])[:10]
        result = [(date, username) for date, (username, _) in top_10_dates]

        logger.info("Top 10 dates with the most tweets retrieved.")
        return result
    
    except Exception as e:
        logger.error(f"An error occurred during memory-optimized processing: {e}")
        return []