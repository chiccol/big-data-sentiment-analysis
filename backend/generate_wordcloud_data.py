import os
from pymongo import MongoClient, UpdateOne

def seed_word_count_db():
    """
    Connects to MongoDB, creates or uses the 'word_count' database,
    then populates sample data for 'apple', 'tesla', and 'google' collections.
    Each company's collection has unique words with counts.
    """

    # 1. Connect to Mongo
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
    client = MongoClient(mongo_uri)

    # 2. Reference the database "word_count"
    db = client["word_count"]

    # 3. Create some sample data for each company
    sample_data = {
        "apple": [
            {"word": "iphone",   "count": 42, "company": "apple"},
            {"word": "macbook",  "count": 15, "company": "apple"},
            {"word": "ios",      "count": 28, "company": "apple"}
        ],
        "tesla": [
            {"word": "model3",   "count": 10, "company": "tesla"},
            {"word": "autopilot","count":  7, "company": "tesla"},
            {"word": "battery",  "count": 16, "company": "tesla"}
        ],
        "google": [
            {"word": "android",  "count": 31, "company": "google"},
            {"word": "search",   "count": 54, "company": "google"},
            {"word": "maps",     "count": 12, "company": "google"}
        ]
    }

    # 4. Loop over each company, prepare upsert operations, and write them
    for company_name, docs in sample_data.items():
        collection = db[company_name]  # e.g. db["apple"] or db["tesla"], etc.

        # Prepare bulk upsert: increment or create doc with {word, count, company}
        updates = []
        for doc in docs:
            word = doc["word"]
            count_val = doc["count"]
            # upsert: if the 'word' doesn't exist, create it; otherwise increment 'count'
            updates.append(
                UpdateOne(
                    {"word": word},  # filter
                    {
                        "$inc": {"count": count_val},
                        "$setOnInsert": {"company": company_name}
                    },
                    upsert=True
                )
            )

        if updates:
            result = collection.bulk_write(updates)
            print(f"Collection '{company_name}':")
            print(f"  Upserted: {result.upserted_count}, Matched: {result.matched_count}, Modified: {result.modified_count}")

            # 5. Create a unique index on the 'word' field (if not already existing)
            collection.create_index([("word", 1)], unique=True)

    client.close()
    print("Seeding completed. Check the 'word_count' database in MongoDB.")

if __name__ == "__main__":
    seed_word_count_db()