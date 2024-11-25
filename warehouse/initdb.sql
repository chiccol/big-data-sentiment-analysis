CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
	source TEXT NOT NULL,
    date TEXT, 
	company TEXT,
    sentiment TEXT NULL, 
    negative_probability FLOAT NULL,
    neutral_probability FLOAT NULL,
    positive_probability FLOAT NULL,
);
