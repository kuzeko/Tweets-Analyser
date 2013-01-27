SELECT
t.tweet_id,
t.user_id,
REPLACE(t.text, '\n', ' '),
i.created_at
INTO OUTFILE '/tmp/EXPORT/1G_tweets_order_by_ID_USER_ASC_Newline.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM tweet_text_01 AS t JOIN tweet_01 AS i  ON t.user_id = i.user_id AND t.tweet_id = i.id ORDER BY t.tweet_id ASC, t.user_id ASC LIMIT 1000000;
