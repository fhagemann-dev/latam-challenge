import polars as pl
from typing import List, Tuple
import emoji
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q2_time")

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Extracts and counts the top 10 most used emojis from a JSON file containing tweets.
    Time-optimized function using Polars.
    
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
        records = []
        emoji_set = set(emoji.EMOJI_DATA.keys())
        with open(file_path, 'r') as file:
            for line in file:
                tweet = json.loads(line)
                content = tweet.get('content', '')
                emojis = [char for char in content if char in emoji_set]
                records.extend(emojis)  # Add each emoji to the records list

        # Convert to Polars DataFrame for faster counting
        df = pl.DataFrame({"emoji": records})
        logger.info("Data loaded into Polars DataFrame.")

        # Count each emoji and get the top 10
        top_10_emojis = (
            df.group_by("emoji")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        result = [(row["emoji"], row["count"]) for row in top_10_emojis.to_dicts()]

        logger.info("Top 10 emojis with their counts calculated successfully.")
        return result

    except Exception as e:
        logger.error(f"An error occurred while processing the emojis: {e}")
        return []