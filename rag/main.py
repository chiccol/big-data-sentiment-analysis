from pymongo import MongoClient
import os
from time import sleep
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from random import sample 

def get_reviews(db, sentiment, company):
    comapny_reviews = db[company]
    reviews = comapny_reviews.find({"sentiment": sentiment})
    reviews = [review["text"] for review in reviews]
    text_splitter = CharacterTextSplitter(
        separator=" ",   # Split by spaces
        chunk_size=200,  # Maximum size of each chunk
        chunk_overlap=0  # Overlap between chunks
        )
    splitted_reviews = [text_splitter.split_text(review) for review in reviews]
    return splitted_reviews

def main():
    print("Wait for mongodb to start and to have some data...", flush=True)
    sleep(30) 
    embeddings = HuggingFaceEmbeddings(
        model_name = "BAAI/bge-large-en"
        )
    # MongoDB connection URI (from environment variables or default)
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
    client = MongoClient(mongo_uri)
    # Access the database and collection
    db_name = "reviews"  # Replace with your database name
    db = client[db_name]
    # Get the names of all collections in the "reviews" database
    queries = [
        "Customer service", 
        "Product quality", 
        "Price",
        "General"
    ]
    answer = {
            "positive": "",
            "neutral": "",
            "negative": ""
        }
    while True:
        print("Retrieving reviews from MongoDB...", flush=True)
        collections = db.list_collection_names() # to be relace with company asking for reviews through UI
        print(f"Got Collections: {collections}", flush=True)
        company = sample(collections, 1)[0]
        # Get all reviews for the current company
        print(f"Company: {company}", flush=True)
        for sentiment in answer:
            print(f"Extracting info for Sentiment: {sentiment}", flush=True)
            reviews = get_reviews(db, sentiment, company)
            vectorstore = FAISS.from_texts(reviews, embeddings)
            for query in queries:
                print(f"Query: {query}", flush=True)
                results = vectorstore.similarity_search(query, k=3)
                results = [result[0].page_content for result in results]
                print(f"Results: {results}", flush=True)
        sleep(40)

if __name__ == "__main__":
    main()