import json
from time import time, sleep
import concurrent.futures as cf
import threading
from utils import *
from schema import *
from psycopg2.pool import ThreadedConnectionPool
from logger import Logger

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", 3))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", 16))

log = Logger()


def process_file(tweets_file_path, max_line: int|None = None):

    line_count = 0
    time_before = time()
    users_batch: list[User] = []
    places_batch: list[Place] = []
    tweets_batch: list[Tweet] = []
    hashtags_batch: list[tuple[int, Hashtag]] = []
    urls_batch: list[tuple[int, Url]] = []
    media_batch: list[tuple[int, Media]] = []
    temp_user_mentions_batch: list[tuple[int, UserMention]] = []

    # 10572/10571/10570 for 1000, 191/83 seconds; without 9962, 9964, 9967 entries 101/89 seconds
    def merge_entities(entities: dict, extended_entities: dict) -> dict:
        if not extended_entities:
            return entities or {}
        if not entities:
            return extended_entities or {}

        merged = entities.copy()
        for key, ext_val in extended_entities.items():
            ent_val = merged.get(key)
            if isinstance(ext_val, list):
                # Merge lists, unique by 'id' if present
                seen_media_ids = set()
                merged_list = []
                for item in (ent_val or []) + ext_val:
                    item_id = item.get('id') if isinstance(item, dict) else None
                    if item_id is not None:
                        if item_id not in seen_media_ids:
                            seen_media_ids.add(item_id)
                            merged_list.append(item)
                    else:
                        merged_list.append(item)
                merged[key] = merged_list
            else:
                merged[key] = ext_val
        return merged

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
                    hashtags_batch.append((_tweet.id, hashtag))
            if _tweet.entities.urls:
                for url in _tweet.entities.urls:
                    urls_batch.append((_tweet.id, url))
            if _tweet.entities.media:
                for media in _tweet.entities.media:
                    media_batch.append((_tweet.id, media))
            if _tweet.entities.user_mentions:
                for user_mention in _tweet.entities.user_mentions:
                    temp_user_mentions_batch.append((_tweet.id, user_mention))

        if _tweet.quoted_status:
            parse_tweet(_tweet.quoted_status)
        if _tweet.retweeted_status:
            parse_tweet(_tweet.retweeted_status)

    def try_insert_with_retries(insert_func, args, batch, _conn, name):
        for i in range(RETRY_LIMIT):
            try:
                insert_func(*args)
                _conn.commit()
                batch.clear()
                return True
            except psycopg2.Error:
                log.error(f"Deadlock with {name}, retrying {i + 1}/{RETRY_LIMIT}", False)
                _conn.rollback()
                sleep(1)
        return False

    try:
        conn = pool.getconn()
        cur = conn.cursor()

        with open(tweets_file_path, 'r') as file:
            for line in file:
                if max_line and line_count >= max_line:
                    break
                # Skip empty lines
                if not line.strip():
                    continue
                tweet_json = json.loads(line)
                # if 'extended_entities' in tweet_json:
                #     tweet_json['entities'] = merge_entities(tweet_json.get('entities', {}),
                #                                             tweet_json['extended_entities'])
                try:
                    tweet = Tweet.model_validate(tweet_json)
                    parse_tweet(tweet)

                except Exception as e:
                    log.error(f"Error parsing tweet JSON: {e}")
                    break
                line_count += 1

                if line_count % BATCH_SIZE == 0:
                    # try 3 times if deadlock detected, if fails, you will commit these with next batch
                    if not try_insert_with_retries(insert_users, (cur, users_batch), users_batch, conn, "users") or \
                        not try_insert_with_retries(insert_places, (cur, places_batch), places_batch, conn, "places") :
                        continue
                    if not try_insert_with_retries(insert_tweets, (cur, tweets_batch), tweets_batch, conn, "tweets"):
                        continue
                    try_insert_with_retries(insert_hashtags_and_link, (cur, hashtags_batch), hashtags_batch, conn, "hashtags")
                    try_insert_with_retries(insert_urls, (cur, urls_batch), urls_batch, conn, "urls")
                    try_insert_with_retries(insert_medias, (cur, media_batch), media_batch, conn, "medias")
                    try_insert_with_retries(insert_temp_user_mentions, (cur, temp_user_mentions_batch), temp_user_mentions_batch, conn, "temp_user_mentions")

        # insert remaining
        some_left = True
        while some_left:
            try:
                insert_users(cur, users_batch)
                insert_places(cur, places_batch)
                insert_tweets(cur, tweets_batch)
                insert_hashtags_and_link(cur, hashtags_batch)
                insert_urls(cur, urls_batch)
                insert_medias(cur, media_batch)
                insert_temp_user_mentions(cur, temp_user_mentions_batch)
                conn.commit()
                some_left = False
            except psycopg2.Error:
                conn.rollback()
                sleep(1)
    except Exception as e:
        log.error(f"Error processing file {tweets_file_path}: {e}")
    finally:
        # return connection to pool
        pool.putconn(conn)
        # Debug print
        time_after = time()
        log.info(f"Inserted {line_count} tweets from {os.path.basename(tweets_file_path)} in {time_after - time_before:.2f} seconds.")


data_dir = "data"
jsonl_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".jsonl")]
pool = ThreadedConnectionPool(minconn=1, maxconn=len(jsonl_files), dsn=get_dsn())

seen_ids = set()
seen_ids_lock = threading.Lock()

total_time_before = time()
with cf.ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
    futures = [executor.submit(process_file, file_path, 1000) for file_path in jsonl_files]
    # to check if all threads went fine
    for future in cf.as_completed(futures):
        try:
            future.result()
        except Exception as e:
            log.error(f"Error in thread: {e}")

total_time_after = time()
log.info(f"Processed {len(jsonl_files)} files in {total_time_after - total_time_before:.2f} seconds.")