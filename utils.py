import os
from dotenv import load_dotenv
import psycopg2
from schema import *

def get_connection():
    load_dotenv()
    db_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }

    return psycopg2.connect(**db_params)


def get_dsn():
    load_dotenv()
    db_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    # Build DSN string
    return " ".join(f"{k}={v}" for k, v in db_params.items() if v)


def build_schema(schema_file_name: str = 'schema.sql', connection = None):
    connection_passed = connection is not None
    if not connection_passed:
        connection = get_connection()

    cursor = connection.cursor()

    with open(schema_file_name, 'r') as f:
        sql_statement = f.read()

    try:
        cursor.execute(sql_statement)
        connection.commit()
        print("Schema created successfully.")
    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        if not connection_passed:
            connection.close()


def cleanup_schema(connection = None):
    connection_passed = connection is not None
    if not connection_passed:
        connection = get_connection()

    cursor = connection.cursor()

    sql_statement = """
    DROP TABLE IF EXISTS tweets CASCADE;
    DROP TABLE IF EXISTS users CASCADE;
    DROP TABLE IF EXISTS places CASCADE;
    DROP TABLE IF EXISTS hashtags CASCADE;
    DROP TABLE IF EXISTS tweet_hashtag CASCADE;
    DROP TABLE IF EXISTS tweet_urls CASCADE;
    DROP TABLE IF EXISTS tweet_media CASCADE;
    DROP TABLE IF EXISTS tweet_user_mentions CASCADE;
    DROP TABLE IF EXISTS temp_tweet_user_mentions CASCADE;
    """

    try:
        cursor.execute(sql_statement)
        connection.commit()
        print("Schema cleaned up successfully.")
    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        if not connection_passed:
            connection.close()


insert_user_query = """
    INSERT INTO users (id, name, screen_name, location, description, followers_count, friends_count, statuses_count, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        screen_name = EXCLUDED.screen_name,
        location = EXCLUDED.location,
        description = EXCLUDED.description,
        followers_count = EXCLUDED.followers_count,
        friends_count = EXCLUDED.friends_count,
        statuses_count = EXCLUDED.statuses_count,
        created_at = EXCLUDED.created_at
    RETURNING id;
    """

def user_to_insert_format(user: User):
    return (user.id, user.name, user.screen_name, user.location, user.description,
            user.followers_count, user.friends_count, user.statuses_count, to_iso_format(user.created_at))

def insert_user(cursor, user: User):
    cursor.execute(insert_user_query, user_to_insert_format(user))
    return cursor.fetchone()[0]

def insert_users(cursor, users: list[User]):
    data = [user_to_insert_format(user) for user in users]
    cursor.executemany(insert_user_query, data)


insert_tweet_query = """
    INSERT INTO tweets (id, created_at, full_text, display_from, display_to, lang, user_id, source, in_reply_to_status_id, quoted_status_id, retweeted_status_id, place_id, retweet_count, favorite_count, possibly_sensitive)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        full_text = EXCLUDED.full_text,
        created_at = EXCLUDED.created_at,
        display_from = EXCLUDED.display_from,
        display_to = EXCLUDED.display_to,
        lang = EXCLUDED.lang,
        user_id = EXCLUDED.user_id,
        source = EXCLUDED.source,
        in_reply_to_status_id = EXCLUDED.in_reply_to_status_id,
        quoted_status_id = EXCLUDED.quoted_status_id,
        retweeted_status_id = EXCLUDED.retweeted_status_id,
        place_id = EXCLUDED.place_id,
        retweet_count = EXCLUDED.retweet_count,
        favorite_count = EXCLUDED.favorite_count,
        possibly_sensitive = EXCLUDED.possibly_sensitive
    RETURNING id;
    """

def tweet_to_insert_format(tweet: Tweet):
    return (tweet.id, tweet.created_at, tweet.full_text, tweet.display_text_range[0], tweet.display_text_range[1], tweet.lang,
            tweet.user.id, tweet.source, tweet.in_reply_to_status_id,
            tweet.quoted_status_id if tweet.quoted_status_id else tweet.quoted_status.id if tweet.quoted_status else None,
            tweet.retweeted_status.id if tweet.retweeted_status else None,
            tweet.place.id if tweet.place else None,
            tweet.retweet_count, tweet.favorite_count, tweet.possibly_sensitive)

def insert_tweet(cursor, tweet: Tweet):
    cursor.execute(insert_tweet_query, tweet_to_insert_format(tweet))
    return cursor.fetchone()[0]


def insert_tweets(cursor, tweets: list[Tweet]):
    data = [tweet_to_insert_format(tweet) for tweet in tweets]
    cursor.executemany(insert_tweet_query, data)


insert_temp_user_mention_query = """
    INSERT INTO temp_tweet_user_mentions (tweet_id, mentioned_user_id, mentioned_screen_name, mentioned_name)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

def user_mention_to_insert_format(tweet_id, user_mention: UserMention):
    return tweet_id, user_mention.id, user_mention.screen_name, user_mention.name

def insert_temp_user_mention(cursor, tweet_id, user_mention: UserMention):
    cursor.execute(insert_temp_user_mention_query, user_mention_to_insert_format(tweet_id, user_mention))

def insert_temp_user_mentions(cursor, tweet_id, user_mentions: list[UserMention]):
    data = [user_mention_to_insert_format(tweet_id, user_mention) for user_mention in user_mentions]
    cursor.executemany(insert_temp_user_mention_query, data)


insert_media_query = """
    INSERT INTO tweet_media (tweet_id, media_id, display_url, expanded_url, media_url, media_url_https, type)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

def insert_media_format(tweet_id, media: Media):
    return tweet_id, media.id, media.display_url, media.expanded_url, media.media_url, media.media_url_https, media.type

def insert_media(cursor, tweet_id, media: Media):
    cursor.execute(insert_media_query, insert_media_format(tweet_id, media))

def insert_medias(cursor, tweet_id, medias: list[Media]):
    data = [insert_media_format(tweet_id, media) for media in medias]
    cursor.executemany(insert_media_query, data)


insert_url_query = """
    INSERT INTO tweet_urls (tweet_id, url, expanded_url, display_url, unwound_url)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

def insert_url_format(tweet_id, url: Url):
    unwound = url.unwound_url
    return (tweet_id,
            url.url,
            url.expanded_url,
            url.display_url,
            unwound.url if unwound else None)


def insert_url(cursor, tweet_id, url: Url):
    cursor.execute(insert_url_query, insert_url_format(tweet_id, url))

def insert_urls(cursor, tweet_id, urls: list[Url]):
    data = [insert_url_format(tweet_id, url) for url in urls]
    cursor.executemany(insert_url_query, data)


def get_or_create_hashtag_id(cursor, hashtag: Hashtag) -> int:
    insert_query = """
    INSERT INTO hashtags (tag)
    VALUES (%s)
    ON CONFLICT (tag) DO NOTHING
    RETURNING id;
    """
    cursor.execute(insert_query, (hashtag.text,))
    result = cursor.fetchone()
    if result:
        return result[0]
    # If already exists, fetch the id
    cursor.execute("SELECT id FROM hashtags WHERE tag = %s", (hashtag.text,))
    return cursor.fetchone()[0]

def insert_hashtag(cursor, tweet_id, hashtag: Hashtag):
    hashtag_id = get_or_create_hashtag_id(cursor, hashtag)
    insert_tweet_hashtag_query = """
    INSERT INTO tweet_hashtag (tweet_id, hashtag_id)
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING;
    """
    cursor.execute(insert_tweet_hashtag_query, (tweet_id, hashtag_id))

# This one is GPT generated
def insert_hashtags_and_link(cursor, tweet_id, hashtags: list[Hashtag]):
    # 1. Insert all hashtags (ignore conflicts)
    insert_query = """
    INSERT INTO hashtags (tag)
    VALUES (%s)
    ON CONFLICT (tag) DO NOTHING;
    """
    tags = [(h.text,) for h in hashtags]
    cursor.executemany(insert_query, tags)

    # 2. Fetch all hashtag IDs at once
    tag_texts = [h.text for h in hashtags]
    if tag_texts:
        select_query = "SELECT id, tag FROM hashtags WHERE tag IN %s"
        cursor.execute(select_query, (tuple(tag_texts),))
        tag_id_map = {tag: id for id, tag in cursor.fetchall()}
    else:
        tag_id_map = {}

    # 3. Batch-insert tweet_hashtag links
    tweet_hashtag_data = [(tweet_id, tag_id_map[h.text]) for h in hashtags if h.text in tag_id_map]
    insert_tweet_hashtag_query = """
    INSERT INTO tweet_hashtag (tweet_id, hashtag_id)
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING;
    """
    cursor.executemany(insert_tweet_hashtag_query, tweet_hashtag_data)


insert_place_query = """
    INSERT INTO places (id, place_type, full_name, country_code, country)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        place_type = EXCLUDED.place_type,
        full_name = EXCLUDED.full_name,
        country_code = EXCLUDED.country_code,
        country = EXCLUDED.country
    RETURNING id;
    """

def place_to_insert_format(place: Place):
    return place.id, place.place_type, place.full_name, place.country_code, place.country

def insert_place(cursor, place: Place) -> int:
    cursor.execute(insert_place_query, place_to_insert_format(place))
    return cursor.fetchone()[0]

def insert_places(cursor, places: list[Place]):
    data = [place_to_insert_format(place) for place in places]
    cursor.executemany(insert_place_query, data)
