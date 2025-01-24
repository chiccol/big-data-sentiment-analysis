CREATE TABLE IF NOT EXISTS predictions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    date TIMESTAMP, 
    company TEXT,
    sentiment TEXT NULL 
);

CREATE TABLE IF NOT EXISTS trustpilot (
    id TEXT PRIMARY KEY,
    stars INT,
    location TEXT,
    FOREIGN KEY (id) REFERENCES predictions(id)
);

CREATE TABLE IF NOT EXISTS youtube (
    id TEXT PRIMARY KEY,
    videoid TEXT,
    like_count INT,
    youtube_reply_count INT,
    negative_probability FLOAT NULL,
    neutral_probability FLOAT NULL,
    positive_probability FLOAT NULL,
    FOREIGN KEY (id) REFERENCES predictions(id)
);

CREATE TABLE IF NOT EXISTS reddit (
    id TEXT PRIMARY KEY,
    subreddit TEXT,
    vote INT,
    reddit_reply_count INT,
    negative_probability FLOAT NULL,
    neutral_probability FLOAT NULL,
    positive_probability FLOAT NULL,
    FOREIGN KEY (id) REFERENCES predictions(id)
);

CREATE INDEX idx_predictions_source ON predictions(source);
CREATE INDEX idx_predictions_date ON predictions(date);
CREATE INDEX idx_predictions_company ON predictions(company);
