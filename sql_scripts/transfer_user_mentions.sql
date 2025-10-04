INSERT INTO tweet_user_mentions (tweet_id, mentioned_user_id, mentioned_screen_name, mentioned_name)
SELECT t.tweet_id, t.mentioned_user_id, t.mentioned_screen_name, t.mentioned_name
FROM temp_tweet_user_mentions t
     JOIN tweets tw ON t.tweet_id = tw.id
     JOIN users u ON t.mentioned_user_id = u.id; -- joining so that it doesn't crash on foreign key violation