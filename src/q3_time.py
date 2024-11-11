import polars as pl
from typing import List, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q3_time")

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Extracts and counts the top 10 most mentioned usernames from a Parquet file containing tweets.
    Time-optimized function.
    
    Parameters
    ----------
    file_path : str
        Path to the Parquet file.

    Returns
    -------
    list
        A list of tuples with the top 10 usernames and their mention counts.
    """
    try:
        # Load the 'mentionedUsers' column directly from the Parquet file
        df = pl.read_parquet(file_path, columns=["mentionedUsers"])
        logger.info("Data loaded into Polars DataFrame from Parquet file.")

        # Explode the 'mentionedUsers' column to extract each mentioned user's username
        mentions_df = (
            df.explode("mentionedUsers")
            .select(
                pl.col("mentionedUsers").struct.field("username").alias("username")
            )
            .drop_nulls("username")  # Remove any rows where 'username' is null
        )

        # Group by 'username' and count occurrences, then sort and get top 10
        top_10_users = (
            mentions_df.group_by("username")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        result = [(row[0], row[1]) for row in top_10_users.rows()]

        logger.info("Top 10 most mentioned users calculated successfully.")
        return result

    except Exception as e:
        logger.error(f"An error occurred while processing the mentions: {e}")
        return []