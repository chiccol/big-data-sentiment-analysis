from transformers import AutoModelForCausalLM, AutoTokenizer
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
import torch
from utils import get_reviews, summarizer

from pymongo import MongoClient

from time import sleep
import os

model = "HuggingFaceTB/SmolLM2-360M-Instruct"
embeddings_model = "BAAI/bge-large-en"
device = "cuda" if torch.cuda.is_available() else "cpu" 
mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
db_name = "reviews"

def main():
    """
    Main function to retrieve reviews from MongoDB, process them, and generate summaries.
    Connects to MongoDB, retrieves reviews, summarizes them, and prints the results.
    In the future it should connect to the UI and send the results back.
    """
    print("Wait for mongodb to start and to have some data...", flush=True)
    sleep(30) 
    print("Loading embeddings...", flush=True)
    embeddings = HuggingFaceEmbeddings(
        model_name = embeddings_model
        )
    # MongoDB connection URI (from environment variables or default)
    print("Connecting to MongoDB...", flush=True)
    client = MongoClient(mongo_uri)
    # Access the database and collection  
    db = client[db_name]
    # Get the names of all collections in the "reviews" database
    topics = [
        "Customer service", 
        "Product quality", 
        "Price",
        "General"
    ]
    answers = {
            "positive": dict(),
            "neutral": dict(),
            "negative": dict()
        }
    print("Loading model...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(model)
    # for multiple GPUs install accelerate and do `model = AutoModelForCausalLM.from_pretrained(checkpoint, device_map="auto")`
    model = AutoModelForCausalLM.from_pretrained(model).to(device)
    
    while True:
        print("Retrieving reviews from MongoDB...", flush=True)
        collections = db.list_collection_names()
        print(f"Got Collections: {collections}", flush=True)
        company = "nordvpn.com" # to be replaced with company asking for reviews through UI
        while company not in collections:
            print(f"Company: {company} not found in the database. Waiting for 5 seconds for data to come...", flush=True)
            sleep(10)
            collections = db.list_collection_names()
        # Get all reviews for the current company
        print(f"Company: {company}", flush=True)
        for sentiment in answers:
            print(f"Extracting info for Sentiment: {sentiment}", flush=True)
            reviews = get_reviews(db, sentiment, company)
            if len(reviews) == 0: 
                for topic in topics:
                    answers[sentiment][topic] = "No reviews found"
                continue
            vectorstore = FAISS.from_texts(reviews, embeddings)
            for topic in topics:
                print(f"Topic: {topic}", flush=True)
                retrieved_reviews = vectorstore.similarity_search(topic, k=3)
                retrieved_reviews = [retrieved_review.page_content for retrieved_review in retrieved_reviews]
                retrieved_reviews = "\n".join(retrieved_reviews)
                summary = summarizer(retrieved_reviews, sentiment, topic, model, tokenizer, device)
                answers[sentiment][topic] = summary
                print("Summary:", answers[sentiment][topic], flush=True)
            print("-"*50, flush=True)

if __name__ == "__main__":
    main()