import json
from time import time
import concurrent.futures as cf
import threading
from utils import *
from schema import *
from logger import Logger

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", 16))

log = Logger()

# --- Global sets and individual locks ---
users_set: set[int] = set()
users_lock = threading.Lock()

places_set: set[str] = set()
places_lock = threading.Lock()

tweets_set: set[int] = set()
tweets_lock = threading.Lock()

hashtags_set: set[str] = set()
hashtags_lock = threading.Lock()

tweet_hashtags_set: set[tuple[int, str]] = set()
tweet_hashtags_lock = threading.Lock()

urls_set: set[tuple[int, str]] = set()
urls_lock = threading.Lock()

media_set: set[tuple[int, int]] = set()
media_lock = threading.Lock()

user_mentions_set: set[tuple[int, int]] = set()
user_mentions_lock = threading.Lock()
# --- End global sets and locks ---

def process_file(tweets_file_path, max_line: int|None = None):
    line_count = 0
    time_before = time()

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
        # Tweets
        with tweets_lock:
            tweets_set.add(_tweet.id)
        # Users
        if _tweet.user:
            with users_lock:
                users_set.add(_tweet.user.id)
        # Places
        if _tweet.place:
            with places_lock:
                places_set.add(_tweet.place.id)
        # Entities
        if _tweet.entities:
            if _tweet.entities.hashtags:
                with hashtags_lock:
                    for hashtag in _tweet.entities.hashtags:
                        hashtags_set.add(hashtag.text.lower())
            if _tweet.entities.urls:
                with urls_lock:
                    for url in _tweet.entities.urls:
                        urls_set.add((_tweet.id, url.url))
            if _tweet.entities.media:
                with media_lock:
                    for media in _tweet.entities.media:
                        media_set.add((_tweet.id, media.id))
            if _tweet.entities.user_mentions:
                with user_mentions_lock:
                    for user_mention in _tweet.entities.user_mentions:
                        user_mentions_set.add((_tweet.id, user_mention.id))
        if _tweet.quoted_status:
            parse_tweet(_tweet.quoted_status)
        if _tweet.retweeted_status:
            parse_tweet(_tweet.retweeted_status)

    try:
        with open(tweets_file_path, 'r') as file:
            for line in file:
                if max_line and line_count >= max_line:
                    break
                # Skip empty lines
                if not line.strip():
                    continue
                tweet_json = json.loads(line)
                if 'extended_entities' in tweet_json:
                    tweet_json['entities'] = merge_entities(tweet_json.get('entities', {}),
                                                            tweet_json['extended_entities'])
                try:
                    tweet = Tweet.model_validate(tweet_json)
                    parse_tweet(tweet)

                except Exception as e:
                    log.error(f"Error parsing tweet JSON: {e}")
                    break
                line_count += 1

    except Exception as e:
        log.error(f"Error processing file {tweets_file_path}: {e}")
    finally:
        # Debug print
        time_after = time()
        log.info(f"Processed {line_count} tweets from {os.path.basename(tweets_file_path)} in {time_after - time_before:.2f} seconds.")


data_dir = "data"
jsonl_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".jsonl")]

total_time_before = time()
with cf.ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
    futures = [executor.submit(process_file, file_path) for file_path in jsonl_files]
    # to check if all threads went fine
    for future in cf.as_completed(futures):
        try:
            future.result()
        except Exception as e:
            log.error(f"Error in thread: {e}")

total_time_after = time()
log.info(f"Unique users: {len(users_set)}, places: {len(places_set)}, tweets: {len(tweets_set)}, hashtags: {len(hashtags_set)}, urls: {len(urls_set)}, media: {len(media_set)}, user_mentions: {len(user_mentions_set)}")
log.info(f"Processed {len(jsonl_files)} files in {total_time_after - total_time_before:.2f} seconds.")