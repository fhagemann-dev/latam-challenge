import polars as pl
from typing import List, Tuple
from datetime import datetime
import json
import logging

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q1_time")

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """ 
    Processes a JSON file to find the top 10 dates with the most tweets 
    and the user with the most posts for each date. 
    """
    try:
        records = []
        with open(file_path, 'r') as f:
            for line in f:
                tweet = json.loads(line)
                date_str = tweet.get("date", "").split("T")[0]
                username = tweet.get("user", {}).get("username")
                if date_str and username:
                    records.append({"date": date_str, "username": username})
        logger.info("Successfully read JSON file and extracted records.")

        df = pl.DataFrame(records)
        df = df.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
        logger.info("Data loaded into Polars DataFrame and date parsed.")

        grouped = df.group_by(["date", "username"]).agg(pl.len().alias("tweet_count"))
        logger.info("Grouped by date and username, counted tweets per group.")

        grouped = grouped.sort(["tweet_count", "username"], descending=[True, False])
        top_by_date = grouped.group_by("date").agg(pl.col("username").first(), pl.col("tweet_count").first())
        logger.info("Top user per date identified.")

        top_10 = top_by_date.sort("tweet_count", descending=True).head(10)
        result = [(row[0], row[1]) for row in top_10.rows()]
        logger.info("Top 10 dates with the most tweets retrieved.")

        return result

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return []