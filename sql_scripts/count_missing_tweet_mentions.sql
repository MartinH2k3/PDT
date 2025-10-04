SELECT COUNT(*) AS missing_tweet_mentions
FROM temp_tweet_user_mentions t
         LEFT JOIN tweets tw ON t.tweet_id = tw.id
WHERE tw.id IS NULL; -- if not 0, I f'd up somewhere