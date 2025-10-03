import json
import psycopg2 as psql
from utils import get_connection
from schema import *

tweets_file_path = "data/coronavirus-tweet-id-2020-08-01-02.jsonl"

conn = get_connection()
cur = conn.cursor()

with open(tweets_file_path, 'r') as file:
    max_line = 1000
    errors = 0
    for line in file:
        max_line -= 1
        if max_line < 0:
            break
        tweet_json = json.loads(line)
        try:
            tweet = Tweet.model_validate(tweet_json)
        except Exception as e:
            print(f"Error parsing tweet JSON: {e}")
            errors += 1
            continue

