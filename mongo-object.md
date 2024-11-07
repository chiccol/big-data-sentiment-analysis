from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

class MongoDB:
    def __init__(self, host='localhost', port=27017):
        self.client = MongoClient(f'mongodb://{host}:{port}')
        self.db_name = None
        self.db = None
        self.collection_name = None
        self.collection = None
        self.user_schema = {
            "_id": ObjectId(),
            "name": str,
            "email": str,
            "age": int,
            "created_at": datetime.now()
        }

    def connect_to_db(self, db_name):
        self.db_name = db_name
        self.db = self.client[self.db_name]

    def create_collection(self, collection_name):
        self.collection_name = collection_name
        self.collection = self.db[self.collection_name]

    def insert_user(self, user_data):
        user = self.user_schema.copy()
        user.update(user_data)
        result = self.collection.insert_one(user)
        print(f"Inserted user with ID: {result.inserted_id}")
    


# Access the message attributes
    print(f"Received message with the following attributes:")
    print(f"  value:      {message.value()}")
    print(f"  key:        {message.key()}")
    print(f"  topic:      {message.topic()}")
    print(f"  partition:  {message.partition()}")
    print(f"  offset:     {message.offset()}")
    print(f"  timestamp:  {message.timestamp()}")
    print(f"  headers:    {message.headers()}")
```

The key attributes available in a message object received through `consumer.poll()` are:

1. `value`: The actual message payload, which can be of any type (bytes, string, etc.).
2. `key`: The message key, which can be used for partitioning the topic.
3. `topic`: The name of the Kafka topic the message belongs to.
4. `partition`: The partition number the message was received from.
5. `offset`: The offset of the message within the partition.
6. `timestamp`: The timestamp of when the message was produced.
7. `headers`: Any headers associated with the message, as a list of (key, value) pairs.

# Generally
Abbiamo bisogno di un oggetto che gestisca tutta quanta la pipeline di mongo, controlli quanti db e collection abbiamo, gestisca i messaggi di kafka 
sapendo routarli nelle corrette colllections
