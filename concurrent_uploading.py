import json
from time import time, sleep
import concurrent.futures as cf
import threading
from utils import *
from schema import *
from psycopg2.pool import ThreadedConnectionPool
from logger import Logger

log = Logger()
pool = ThreadedConnectionPool(minconn=1, maxconn=50, dsn=get_dsn())

seen_ids = set()
seen_ids_lock = threading.Lock()

def process_file(tweets_file_path, max_line):
    conn = pool.getconn()
    cur = conn.cursor()
    line_count = 0
    time_before = time()
    users_batch = []
    places_batch = []
    tweets_batch = []
    hashtags_batch = []
    urls_batch = []
    media_batch = []
    temp_user_mentions_batch = []

    def parse_tweet(_tweet: Tweet):
        with seen_ids_lock:
            if _tweet.id in seen_ids:
                return
            seen_ids.add(_tweet.id)

        if _tweet.user:
            users_batch.append(_tweet.user)
        if _tweet.place:
            places_batch.append(_tweet.place)
        tweets_batch.append(_tweet)
        if _tweet.entities:
            if _tweet.entities.hashtags:
                for hashtag in _tweet.entities.hashtags:
                    hashtags_batch.append(hashtag)
            if _tweet.entities.urls:
                for url in _tweet.entities.urls:
                    urls_batch.append(url)
            if _tweet.entities.media:
                for media in _tweet.entities.media:
                    media_batch.append(media)
            if _tweet.entities.user_mentions:
                for user_mention in _tweet.entities.user_mentions:
                    temp_user_mentions_batch.append(user_mention)

        if _tweet.quoted_status:
            parse_tweet(_tweet.quoted_status)
        if _tweet.retweeted_status:
            parse_tweet(_tweet.retweeted_status)


    with open(tweets_file_path, 'r') as file:
        for line in file:
            if line_count >= max_line:
                break
            tweet_json = json.loads(line)
            try:
                tweet = Tweet.model_validate(tweet_json)
                parse_tweet(tweet)

            except Exception as e:
                #print(json.dumps(tweet_json, indent=2))
                print(f"Error parsing tweet JSON: {e}")
                break
            line_count += 1

            if line_count % 100 == 0:
                # try 3 times if deadlock detected, if fails, you will commit these with next batch
                for insert_func, args, batch in [
                    (insert_users, (cur, users_batch), users_batch),
                    (insert_places, (cur, places_batch), places_batch),
                    (insert_tweets, (cur, tweets_batch), tweets_batch),
                    (insert_hashtags_and_link, (cur, tweet.id, hashtags_batch), hashtags_batch),
                    (insert_urls, (cur, tweet.id, urls_batch), urls_batch),
                    (insert_medias, (cur, tweet.id, media_batch), media_batch),
                    (insert_temp_user_mentions, (cur, tweet.id, temp_user_mentions_batch), temp_user_mentions_batch),
                ]:
                    for i in range(3):
                        try:
                            insert_func(*args)
                            conn.commit()
                            batch.clear()  # Only clear if successful
                            break
                        except psycopg2.Error:
                            log.error(f"Deadlock with {insert_func.__name__}, retrying {i+1}/3")
                            conn.rollback()
                            sleep(1)

    # insert remaining
    some_left = True
    while some_left:
        try:
            insert_users(cur, users_batch)
            insert_places(cur, places_batch)
            insert_tweets(cur, tweets_batch)
            insert_hashtags_and_link(cur, tweet.id, hashtags_batch)
            insert_urls(cur, tweet.id, urls_batch)
            insert_medias(cur, tweet.id, media_batch)
            insert_temp_user_mentions(cur, tweet.id, temp_user_mentions_batch)
            conn.commit()
            some_left = False
        except psycopg2.Error:
            conn.rollback()
            sleep(1)

    # return connection to pool
    pool.putconn(conn)

    # Debug print
    time_after = time()
    print(f"Inserted {line_count} tweets from {os.path.basename(tweets_file_path)} in {time_after - time_before:.2f} seconds.")


data_dir = "data"
max_workers = 16

jsonl_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".jsonl")]

total_time_before = time()
with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(process_file, file_path, 50) for file_path in jsonl_files]
    # to check if all threads went fine
    for future in cf.as_completed(futures):
        future.result()

total_time_after = time()
print(f"Processed {len(jsonl_files)} files in {total_time_after - total_time_before:.2f} seconds.")