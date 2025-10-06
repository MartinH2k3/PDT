import json
import csv
from time import time, sleep
import concurrent.futures as cf
import threading
from schema import *
import os
from logger import Logger

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", 3))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", 16))

log = Logger("csv_log.txt")

data_dir = "data"
jsonl_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".jsonl")]

users_set: set[int] = set()
users_lock = threading.Lock()

places_set: set[str] = set()
places_lock = threading.Lock()

tweets_set = set()
tweets_lock = threading.Lock()

hashtags_set: set[str] = set()
hashtags_lock = threading.Lock()

tweet_hashtags_set: set[tuple[int, str]] = set()
tweet_hashtags_lock = threading.Lock()

urls_set: set[tuple[int, str]] = set()
urls_lock = threading.Lock()

media_set: set[tuple[int, int]] = set()
media_lock = threading.Lock()

def process_file(tweets_file_path, max_line: int|None = None):
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
        # tweets
        with tweets_lock:
            tweets_set.add(_tweet.id)

        nonlocal users, places, tweets, hashtags_list, urls, media, user_mentions

        # users
        sender = _tweet.user
        with users_lock:
            if sender.id in users_set:
                pass
            else:
                users_set.add(sender.id)
                users.append([
                    str(sender.id),
                    f'"{sender.screen_name or ""}"',
                    f'"{sender.name or ""}"',
                    "C",#sender.description or '',
                    str(sender.verified) if sender.verified is not None else '',
                    str(sender.protected) if sender.protected is not None else '',
                    str(sender.followers_count) if sender.followers_count is not None else '',
                    str(sender.friends_count) if sender.friends_count is not None else '',
                    str(sender.statuses_count) if sender.statuses_count is not None else '',
                    to_iso_format(sender.created_at) if sender.created_at else '',
                    f'"{sender.location or ""}"',
                    f'"{sender.url or ""}"'
                ])

        # places
        if _tweet.place:
            with places_lock:
                if _tweet.place.id in places_set:
                    pass
                else:
                    places_set.add(_tweet.place.id)
                    places.append([
                        _tweet.place.id or '',
                        f'"{_tweet.place.full_name or ""}"',
                        f'"{_tweet.place.country or ""}"',
                        f'"{_tweet.place.country_code or ""}"',
                        f'"{_tweet.place.place_type or ""}"'
                    ])

        # tweets (already checked above, just append)
        tweets.append([
            str(_tweet.id),
            to_iso_format(_tweet.created_at) if _tweet.created_at else '',
            'B', #_tweet.full_text or '',
            str(_tweet.display_text_range[0]) or '',
            str(_tweet.display_text_range[1]) or '',
            f'"{_tweet.lang or ""}"',
            str(_tweet.user.id) if _tweet.user else '',
            f'"{_tweet.source or ""}"',
            str(_tweet.in_reply_to_status_id) if _tweet.in_reply_to_status_id is not None else '',
            str(_tweet.quoted_status_id) if _tweet.quoted_status_id is not None else '',
            str(_tweet.retweeted_status.id) if _tweet.retweeted_status is not None else '',
            _tweet.place.id if _tweet.place else '',
            str(_tweet.retweet_count) if _tweet.retweet_count is not None else '',
            str(_tweet.favorite_count) if _tweet.favorite_count is not None else '',
            str(_tweet.possibly_sensitive) if _tweet.possibly_sensitive is not None else ''
        ])

        # hashtags
        if _tweet.entities and _tweet.entities.hashtags:
            for h in _tweet.entities.hashtags:
                with hashtags_lock:
                    key = h.text or ''
                    if key in hashtags_set:
                        pass
                    else:
                        hashtags_set.add(key)
                        hashtags_list.append([
                            str(_tweet.id),
                            f'"{h.text or ""}"'
                        ])

        # urls
        if _tweet.entities and _tweet.entities.urls:
            for u in _tweet.entities.urls:
                with urls_lock:
                    key = (int(_tweet.id), u.url or '')
                    if key in urls_set:
                        pass
                    else:
                        urls_set.add(key)
                        urls.append([
                            str(_tweet.id),
                            f'"{u.url or ""}"',
                            f'"{u.expanded_url or ""}"',
                            f'"{u.display_url or ""}"',
                            f'"{u.unwound_url or ""}"'
                        ])

        # user_mentions (no set/lock for user_mentions, as no global set is defined)
        if _tweet.entities and _tweet.entities.user_mentions:
            for um in _tweet.entities.user_mentions:
                user_mentions.append([
                    str(_tweet.id),
                    str(um.id) if um.id is not None else '',
                    f'"{um.screen_name or ""}"',
                    f'"{um.name or ""}"'
                ])

        # media
        if _tweet.entities and _tweet.entities.media:
            for m in _tweet.entities.media:
                with media_lock:
                    key = (int(_tweet.id), int(m.id) if m.id is not None else 0)
                    if key in media_set:
                        pass
                    else:
                        media_set.add(key)
                        media.append([
                            str(_tweet.id),
                            str(m.id) if m.id is not None else '',
                            f'"{m.type or ""}"',
                            f'"{m.media_url or ""}"',
                            f'"{m.media_url_https or ""}"',
                            f'"{m.display_url or ""}"',
                            f'"{m.expanded_url or ""}"'
                        ])

        # nested tweets
        if _tweet.quoted_status:
            parse_tweet(_tweet.quoted_status)
        if _tweet.retweeted_status:
            parse_tweet(_tweet.retweeted_status)

    users: list[list[str]] = []
    places: list[list[str]] = []
    tweets: list[list[str]] = []
    hashtags_list: list[list[str]] = []
    urls: list[list[str]] = []
    media: list[list[str]] = []
    user_mentions: list[list[str]] = []

    time_before = time()

    base_name = os.path.basename(tweets_file_path)[29:]
    base_file_name = os.path.splitext(base_name)[0]
    line_count = 0
    try:
        with open(tweets_file_path, 'r') as file:
            for line in file:
                if not line.strip():
                    continue

                line_count += 1

                if max_line and line_count > max_line:
                    break

                tweet_json = json.loads(line)

                if 'extended_entities' in tweet_json:
                    tweet_json['entities'] = merge_entities(tweet_json.get('entities', {}), tweet_json['extended_entities'])

                try:
                    tweet = Tweet.model_validate(tweet_json)
                    parse_tweet(tweet)
                except Exception as e:
                    log.error(f"Error parsing tweet JSON: {e}")
                    break

            if line_count % BATCH_SIZE == 0:
                tables = [ ("users", users), ("places", places), ("tweets", tweets), ("hashtags", hashtags_list), ("urls", urls), ("media", media), ("user_mentions", user_mentions) ]
                for table_name, table_content in tables:
                    with open(f"output/{base_file_name}_{table_name}.csv", 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerows(table_content)
                        log.info(f"Wrote to {base_file_name}_{table_name}.csv")
                # clean up
                for _, table_content in tables:
                    table_content.clear()

        # insert the remainder
        tables = [("users", users), ("places", places), ("tweets", tweets), ("hashtags", hashtags_list), ("urls", urls),
                  ("media", media), ("user_mentions", user_mentions)]
        for table_name, table_content in tables:
            with open(f"output/{base_file_name}_{table_name}.csv", 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(table_content)

    except Exception as e:
        log.error(f"Error processing file {tweets_file_path}: {e}")
        return

    finally:
        time_after = time()
        log.info(f"Processed {line_count-1} tweets from {base_name} in {time_after - time_before:.2f} seconds.")


# clean up the csv files first
for file_path in jsonl_files:
    base_name = os.path.basename(file_path)[29:]
    base_file_name = os.path.splitext(base_name)[0]
    for table in ["users", "places", "tweets", "hashtags", "urls", "media", "user_mentions"]:
        csv_file_path = f"output/{base_file_name}_{table}.csv"
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)


total_time_before = time()
with cf.ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
    futures = [executor.submit(process_file, file_path) for file_path in jsonl_files]
    for future in cf.as_completed(futures):
        try:
            future.result()
        except Exception as e:
            log.error(f"Error in thread: {e}")

total_time_after = time()
log.info(f"All files processed in {total_time_after - total_time_before:.2f} seconds.")

time_before_joining_csv_files = time()
# join all csv files into one for each table
for table in ["users", "places", "tweets", "hashtags", "urls", "media", "user_mentions"]:
    with open(f"output/{table}.csv", 'w', newline='', encoding='utf-8') as outfile:
        for file_path in jsonl_files:
            base_name = os.path.basename(file_path)[29:]
            base_file_name = os.path.splitext(base_name)[0]
            csv_file_path = f"output/{base_file_name}_{table}.csv"
            if os.path.exists(csv_file_path):
                with open(csv_file_path, 'r', encoding='utf-8') as infile:
                    file_content = infile.read()
                    outfile.write(file_content)
                os.remove(csv_file_path)  # remove the individual file after merging