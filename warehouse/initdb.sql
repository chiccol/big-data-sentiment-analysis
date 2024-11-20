CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
	source TEXT NOT NULL,
    text TEXT NOT NULL,
    date TEXT,
    tp_stars INTEGER,
    tp_location TEXT,
    yt_videoid TEXT,
    yt_like_count INTEGER,
    yt_reply_count INTEGER,
	kafka_topic TEXT
);
