from pymongo import MongoClient
# Connect to MongoDB running in Docker
client = MongoClient('mongodb://localhost:35001/')

# Create or connect to a database
db = client.mongodb  # Replace 'my_database' with your DB name
collection_name = 'db'  

# Create a collection (table equivalent in MongoDB)
collection = db[collection_name]
  # Replace 'my_collection' with your collection name


yt_consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'None',  # Name of the consumer group for dynamic partition assignment
    'auto.offset.reset': 'earliest',  # Start from the earliest message if no valid offset is provided
    'enable.auto.commit': True,  # Enable automatic offset committing
    'enable.auto.offset.store': 'false', #This + last setting ensure "At least once" guarantee
    'auto.commit.interval.ms': 1000  # Interval for committing offsets
    })
