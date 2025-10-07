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

-- Temporary table for nonexistent users referenced in tweet_user_mentions
CREATE TABLE temp_users (
    id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE places (
    id TEXT PRIMARY KEY,
    full_name TEXT,
    country TEXT,
    country_code TEXT,
    place_type TEXT
);

CREATE TABLE tweets (
    id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    full_text TEXT,
    display_from INT,
    display_to INT,
    lang TEXT,
    user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    source TEXT,
    in_reply_to_status_id BIGINT, -- soft foreign key
    quoted_status_id BIGINT,      -- soft foreign key
    retweeted_status_id BIGINT,   -- soft foreign key
    place_id TEXT REFERENCES places(id),
    retweet_count INT,
    favorite_count INT,
    possibly_sensitive BOOLEAN
);

CREATE TABLE hashtags (
    id BIGSERIAL PRIMARY KEY,
    tag TEXT UNIQUE
);

CREATE TABLE tweet_hashtag (
    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
    hashtag_id BIGINT REFERENCES hashtags(id) ON DELETE CASCADE,
    PRIMARY KEY (tweet_id, hashtag_id)
);

CREATE TABLE tweet_urls (
    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
    url TEXT,
    expanded_url TEXT,
    display_url TEXT,
    unwound_url TEXT,
    PRIMARY KEY (tweet_id, url)
);

CREATE TABLE tweet_user_mentions (
    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
    mentioned_user_id BIGINT REFERENCES users(id),
    mentioned_screen_name TEXT,
    mentioned_name TEXT,
    PRIMARY KEY (tweet_id, mentioned_user_id)
);

CREATE TABLE tweet_media (
    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
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

CREATE INDEX idx_tweet_media_tweet_id ON tweet_media(tweet_id);

CREATE INDEX idx_hashtags_tag ON hashtags(tag);