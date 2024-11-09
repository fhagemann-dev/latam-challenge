import logging
import json
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q1_memory")

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """ 
    The function examines a JSON file containing tweets and returns the top 10 dates with the 
    most tweets and the user with the most posts for each of those days. Memory-optimized function.
    
    Parameters
    ----------
    file_path : str
        Path to the JSON file.

    Returns
    -------
    list
        A list of tuples with the top ten dates and the username with the most published 
        tweets on each of those days.
    """
    try:
        # Dictionary to store counts by date and user
        date_user_count = defaultdict(lambda: defaultdict(int))
        logger.info("Initialized date-user counter.")
        
        # Open the file and read it line by line
        with open(file_path, 'r') as f:
            for line in f:
                tweet = json.loads(line)
                
                date_str = tweet.get("date", "").split("T")[0]
                username = tweet.get("user", {}).get("username")
                
                # If date and username are valid, increment the count
                if date_str and username:
                    date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    date_user_count[date][username] += 1

        top_dates = sorted(date_user_count.items(), key=lambda x: sum(x[1].values()), reverse=True)[:10]
        logger.info("Identified top 10 dates with the most tweets.")

        result = [(date, max(users, key=users.get)) for date, users in top_dates]
        logger.info("Determined most active user for each top date.")
        
        logger.info("Memory-optimized processing completed.")
        return result
    
    except Exception as e:
        logger.error(f"An error occurred during memory-optimized processing: {e}")
        return []