import polars as pl
from typing import List, Tuple
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q3_time")

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Extracts and counts the top 10 most mentioned usernames from a JSON file containing tweets.
    Time-optimized function using Polars.
    
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
        mentions = []

        # Read the JSON file line by line to avoid full-file loading
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    tweet = json.loads(line)
                    mentioned_users = tweet.get('mentionedUsers')
                    
                    # Extract usernames from each mentioned user
                    if mentioned_users:
                        for user_info in mentioned_users:
                            user = user_info.get('username')
                            if user:
                                mentions.append(user)
                except json.JSONDecodeError:
                    logger.warning("Skipped a line due to JSON decoding error.")

        # Convert list of mentions into a Polars DataFrame for counting
        df = pl.DataFrame({"username": mentions})
        logger.info("Data loaded into Polars DataFrame for counting.")

        # Group by 'username' and count occurrences, then sort and get top 10
        top_10_users = (
            df.group_by("username")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .limit(10)
        )
        result = [(row["username"], row["count"]) for row in top_10_users.to_dicts()]

        logger.info("Top 10 most mentioned users calculated successfully.")
        return result

    except Exception as e:
        logger.error(f"An error occurred while processing the mentions: {e}")
        return []