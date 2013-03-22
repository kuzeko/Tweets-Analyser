SELECT
t.tweet_id,
t.user_id,
REPLACE(t.text, '\n', ' ')
INTO OUTFILE '/tmp/EXPORT/1G_tweets.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM tweet_text_01 AS t ORDER BY t.tweet_id DESC, t.user_id DESC LIMIT 1000000;


SELECT
i.id,
i.user_id,
i.created_at
INTO OUTFILE '/tmp/EXPORT/1G_dates.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM tweet_01 AS i ORDER BY i.id DESC, i.user_id DESC LIMIT 1000000;


SELECT
*
INTO OUTFILE '/tmp/EXPORT/ALL_hashtags.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM tweet_hashtag ORDER BY tweet_id DESC, user_id DESC;


CREATE TABLE temp_tweet_hashtag AS
SELECT
h.tweet_id,
h.user_id,
h.hashtag_id
FROM tweet_hashtag  AS h INNER JOIN
(SELECT i.id, i.user_id FROM tweet_01 AS i ORDER BY i.id DESC, i.user_id DESC LIMIT 1000000) as t
ON h.tweet_id = t.id AND h.user_id = t.user_id
ORDER BY tweet_id DESC, user_id DESC;


SELECT
h.tweet_id,
h.user_id,
h.hashtag_id
INTO OUTFILE '/tmp/EXPORT/1G_hashtags.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM temp_tweet_hashtag  AS h
ORDER BY tweet_id DESC, user_id DESC;


SELECT
t.tweet_id,
t.user_id,
REPLACE(t.text, '\n', ' ')
INTO OUTFILE '/tmp/EXPORT/ALL_tweets.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM tweet_text_01 AS t;

SELECT
i.id,
i.user_id,
i.created_at
INTO OUTFILE '/tmp/EXPORT/ALL_dates.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM tweet_01 AS i;
