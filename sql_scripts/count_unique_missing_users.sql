SELECT COUNT(DISTINCT t.mentioned_user_id) AS unique_missing_users
FROM temp_tweet_user_mentions t
         LEFT JOIN users u ON t.mentioned_user_id = u.id
WHERE u.id IS NULL;