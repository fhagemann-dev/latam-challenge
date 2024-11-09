import logging
from pyspark.sql.functions import col, to_date, count, desc, row_number
from pyspark.sql.window import Window
from typing import List, Tuple
import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("q1_time")

def q1_time(spark, file_path: str) -> List[Tuple[datetime.date, str]]:
    """ 
    The function examines a JSON file containing tweets and returns the top 10 dates with the 
    most tweets and the user with the most posts for each of those days. Time-optimized function.
    
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
        # Read JSON data into a Spark DataFrame, selecting only relevant columns
        df = spark.read.json(file_path).select("date", "user.username")
        logger.info("Data loaded successfully from JSON file with essential columns selected.")

        df = df.withColumn("tweet_date", to_date(col("date").substr(1, 10), "yyyy-MM-dd"))
        logger.info("Date column converted to date format.")

        # Group by date and username to count tweets, then use a window to rank users by tweet count
        date_user_counts = (df.groupBy("tweet_date", "username")
                            .agg(count("*").alias("tweet_count"))
                            .orderBy(desc("tweet_count"))
                            .limit(10))
        logger.info("Grouped by tweet_date and username, and counted tweets per user per day.")

        # Define a window partitioned by date and ordered by tweet count in descending order
        window_spec = Window.partitionBy("tweet_date").orderBy(desc("tweet_count"))
        
        # Rank users within each date partition and get only the top user per date
        top_user_per_date = (date_user_counts
                             .withColumn("rank", row_number().over(window_spec))
                             .filter(col("rank") == 1)
                             .select("tweet_date", "username", "tweet_count")
                             .limit(10))  # Limit to top 10 dates after ranking
        logger.info("Identified the most active user per date.")

        # Collect the result as a list of tuples
        result = [(row["tweet_date"], row["username"]) for row in top_user_per_date.collect() if row["tweet_date"]]
        logger.info("Result successfully collected.")

        return result
    
    except Exception as e:
        logger.error(f"An error occurred while processing the data: {e}")
        return []