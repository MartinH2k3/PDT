CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    screen_name TEXT,
    name TEXT,
    description TEXT,
    verified BOOLEAN,
    protected BOOLEAN,
    followers_count INT,
    friends_count INT,
    statuses_count INT,
    created_at TIMESTAMP,
    location TEXT,
    url TEXT
);

CREATE TABLE temp_users (
    id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE
);

-- PLACES table
CREATE TABLE places (
    id TEXT PRIMARY KEY,
    full_name TEXT,
    country TEXT,
    country_code TEXT,
    place_type TEXT
);

-- TWEETS table
CREATE TABLE tweets (
    id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    full_text TEXT,
    display_from INT,
    display_to INT,
    lang TEXT,
    user_id BIGINT,
    source TEXT,
    in_reply_to_status_id BIGINT, -- soft foreign key
    quoted_status_id BIGINT,      -- soft foreign key
    retweeted_status_id BIGINT,   -- soft foreign key
    place_id TEXT,
    retweet_count INT,
    favorite_count INT,
    possibly_sensitive BOOLEAN
);

-- HASHTAGS table
CREATE TABLE hashtags (
    id BIGSERIAL PRIMARY KEY,
    tag TEXT UNIQUE
);

-- TWEET_HASHTAG table (many-to-many)
CREATE TABLE tweet_hashtag (
    tweet_id BIGINT,
    hashtag_id TEXT,
    PRIMARY KEY (tweet_id, hashtag_id)
);

-- TWEET_URLS table
CREATE TABLE tweet_urls (
    tweet_id BIGINT,
    url TEXT,
    expanded_url TEXT,
    display_url TEXT,
    unwound_url TEXT,
    PRIMARY KEY (tweet_id, url)
);

-- TWEET_USER_MENTIONS table
CREATE TABLE tweet_user_mentions (
    tweet_id BIGINT,
    mentioned_user_id BIGINT,
    mentioned_screen_name TEXT,
    mentioned_name TEXT,
    PRIMARY KEY (tweet_id, mentioned_user_id)
);

-- TWEET_MEDIA table
CREATE TABLE tweet_media (
    tweet_id BIGINT,
    media_id BIGINT,
    type TEXT,
    media_url TEXT,
    media_url_https TEXT,
    display_url TEXT,
    expanded_url TEXT,
    PRIMARY KEY (tweet_id, media_id)
);

-- Indexes for foreign keys to speed up joins and ON DELETE operations
CREATE INDEX idx_tweets_user_id ON tweets(user_id);
CREATE INDEX idx_tweets_place_id ON tweets(place_id);
CREATE INDEX ix_tweets_in_reply_to_status_id ON tweets(in_reply_to_status_id);
CREATE INDEX ix_tweets_quoted_status_id ON tweets(quoted_status_id);
CREATE INDEX ix_tweets_retweeted_status_id ON tweets(retweeted_status_id);

CREATE INDEX idx_tweet_hashtag_tweet_id ON tweet_hashtag(tweet_id);
CREATE INDEX idx_tweet_hashtag_hashtag_id ON tweet_hashtag(hashtag_id);

CREATE INDEX idx_tweet_urls_tweet_id ON tweet_urls(tweet_id);

CREATE INDEX idx_tweet_user_mentions_tweet_id ON tweet_user_mentions(tweet_id);
CREATE INDEX idx_tweet_user_mentions_mentioned_user_id ON tweet_user_mentions(mentioned_user_id);

CREATE INDEX idx_temp_tweet_user_mentions_tweet_id ON temp_tweet_user_mentions(tweet_id);
CREATE INDEX idx_temp_tweet_user_mentions_mentioned_user_id ON temp_tweet_user_mentions(mentioned_user_id);

CREATE INDEX idx_tweet_media_tweet_id ON tweet_media(tweet_id);

CREATE INDEX idx_hashtags_tag ON hashtags(tag);