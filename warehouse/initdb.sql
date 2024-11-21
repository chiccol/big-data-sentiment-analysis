CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
	source TEXT NOT NULL,
    date TEXT,
	kafka_topic TEXT
);
