-- "users", "places", "tweets", "hashtags", "urls", "media", "user_mentions"]
COPY users FROM 'C:\Users\marti\PycharmProjects\PDT\output\users.csv' DELIMITER ',' CSV;
COPY temp_users FROM 'C:\Users\marti\PycharmProjects\PDT\output\temp_users.csv' DELIMITER ',' CSV;
COPY places FROM 'C:\Users\marti\PycharmProjects\PDT\output\places.csv' DELIMITER ',' CSV;
COPY tweets FROM 'C:\Users\marti\PycharmProjects\PDT\output\tweets.csv' DELIMITER ',' CSV;
COPY hashtags FROM 'C:\Users\marti\PycharmProjects\PDT\output\hashtags.csv' DELIMITER ',' CSV;
COPY tweet_hashtag FROM 'C:\Users\marti\PycharmProjects\PDT\output\tweet_hashtag.csv' DELIMITER ',' CSV;
COPY tweet_urls FROM 'C:\Users\marti\PycharmProjects\PDT\output\urls.csv' DELIMITER ',' CSV;
COPY tweet_user_mentions FROM 'C:\Users\marti\PycharmProjects\PDT\output\user_mentions.csv' DELIMITER ',' CSV;
COPY tweet_media FROM 'C:\Users\marti\PycharmProjects\PDT\output\media.csv' DELIMITER ',' CSV;

-- Remove temporary non-existent users
DELETE FROM users WHERE id IN (SELECT id FROM temp_users);