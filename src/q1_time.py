import polars as pl
import logging
from typing import List, Tuple
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q1_time")

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """ 
    Processes a Parquet file to find the top 10 dates with the most tweets 
    and the user with the most posts for each date. Time-optimzed function.
    
    Parameters
    ----------
    file_path : str
        Path to the parquet files.

    Returns
    -------
    list
        A list of tuples with the top 10 dates and the username with the most published 
        tweets on each of those dates.
    """
    
    try:
        # Load the Parquet file as a Polars DataFrame
        df = pl.read_parquet(file_path)
        logger.info(f"Available columns in DataFrame: {df.columns}")

        df = df.with_columns(pl.col("user").struct.field("username").alias("username"))
        logger.info("Extracted 'username' from 'user' struct column.")

        # Parse the 'date' column to a date format if it includes timestamps
        df = df.with_columns(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False).dt.date()
        )

        # Group by date and username, counting tweets per group
        grouped = df.group_by(["date", "username"]).agg(pl.len().alias("tweet_count"))

        # Sort by tweet count (descending) and username (ascending) to handle ties
        grouped = grouped.sort(["tweet_count", "username"], descending=[True, False])

        # Get the top user per date by grouping by date and selecting the first entry
        top_by_date = grouped.group_by("date").agg(pl.col("username").first(), pl.col("tweet_count").first())
        logger.info("Top user per date identified.")

        # Sort the results by tweet_count to find the top 10 dates overall
        top_10 = top_by_date.sort("tweet_count", descending=True).head(10)

        # Finally, collect results as a list of tuples
        result = [(row[0], row[1]) for row in top_10.rows()]
        logger.info("Top 10 dates with the most tweets retrieved.")

        return result

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return []