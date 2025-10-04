SELECT COUNT(*) AS missing_user_mentions
FROM temp_tweet_user_mentions t
         LEFT JOIN users u ON t.mentioned_user_id = u.id
WHERE u.id IS NULL;