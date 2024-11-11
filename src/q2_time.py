import polars as pl
from typing import List, Tuple
import emoji
import logging

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q2_time")

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Extracts and counts the top 10 most used emojis from a parquet file containing tweets.
    Time-optimized function.
    
    Parameters
    ----------
    file_path : str
        Path to the Parquet file.

    Returns
    -------
    list
        A list of tuples with the top 10 emojis and their counts.
    """
    try:
        df = pl.read_parquet(file_path, columns=["content"])
        logger.info("Data loaded into Polars DataFrame from Parquet file.")

        # Define the set of emojis from emoji.EMOJI_DATA
        emoji_set = set(emoji.EMOJI_DATA.keys())

        # Extract emojis from each 'content' field
        emojis_list = [
            char for content in df["content"] for char in content if char in emoji_set
        ]

        emoji_df = pl.DataFrame({"emoji": emojis_list})
        
        # Group by emoji and count occurrences
        top_10_emojis = (
            emoji_df.group_by("emoji")
            .agg(pl.col("emoji").count().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        result = [(row[0], row[1]) for row in top_10_emojis.rows()]

        logger.info("Top 10 emojis with their counts calculated successfully.")
        return result

    except Exception as e:
        logger.error(f"An error occurred while processing the emojis: {e}")
        return []