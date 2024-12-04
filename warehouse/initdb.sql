CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    date TEXT, 
    company TEXT,
    sentiment TEXT NULL, 
    negative_probability FLOAT NULL,
    neutral_probability FLOAT NULL,
    positive_probability FLOAT NULL,
    tp_stars INT NULL, 
    tp_location TEXT NULL, 
    yt_videoid TEXT NULL,
    yt_like_count INT NULL,
    yt_reply_count INT NULL,
	re_subreddit TEXT NULL,
	re_id TEXT NULL,
	re_vote INT NULL,
	re_reply_count INT NULL
);

git commit -m "Added reddit. Added parquet encoding to reddit. Added its fields to the schema for decoding in spark. Removed from docker compose mongo-controller. There is still an issue where the company from reddit is Null, I had to change the spark dataframe schema allowing company to be nullable to circumvent this, but it is not doable, it must be non-null."
