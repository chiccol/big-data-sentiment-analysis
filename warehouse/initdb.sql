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
