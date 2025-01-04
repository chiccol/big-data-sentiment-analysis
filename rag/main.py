from transformers import AutoModelForCausalLM, AutoTokenizer
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
import torch
from utils import get_reviews, summarizer

from pymongo import MongoClient

from time import sleep
import logging
from config import CONFIG

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("RAG")
logger.info("Started logging")

def main():
    """
    Main function to retrieve reviews from MongoDB, process them, and generate summaries.
    Connects to MongoDB, retrieves reviews splitted in chnks, summarizes them, and prints the results.
    In the future it should connect to the UI and send the results back.
    """
    logger.info("Wait for mongodb to start and to have some data...", flush=True)
    sleep(30) 
    logger.info("Loading embeddings...", flush=True)
    embeddings = HuggingFaceEmbeddings(
        model_name = CONFIG["embeddings_model"]
        )
    # MongoDB connection URI (from environment variables or default)
    logger.info("Connecting to MongoDB...", flush=True)
    client = MongoClient(CONFIG["mongo_uri"])
    # Access the database and collection  
    db = client[CONFIG["db_name"]]
    # Get the names of all collections in the "reviews" database
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Loading model on {device}", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(CONFIG["conv_model"])
    # for multiple GPUs install accelerate and do `model = AutoModelForCausalLM.from_pretrained(checkpoint, device_map="auto")`
    model = AutoModelForCausalLM.from_pretrained(CONFIG["conv_model"]).to(device)
    logger.info("Generation model:", CONFIG["conv_model"], flush=True)
    logger.info("Embeddings model:", CONFIG["embeddings_model"], flush=True)
    logger.info("Retrieving by the following topics:", CONFIG["topics"], flush=True)
    
    while True:
        # Dictionary to store the answers
        answers = {
            "positive": dict(),
            "neutral": dict(),
            "negative": dict()
        }
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
            reviews = get_reviews(db, sentiment, company, CONFIG["chunk_size"], CONFIG["chunk_overlap"], CONFIG["separator"])
            # If no reviews are found, add a message to the answers
            if len(reviews) == 0: 
                for topic in CONFIG["topics"]:
                    answers[sentiment][topic] = "No reviews found"
                continue
            vectorstore = FAISS.from_texts(reviews, embeddings)
            for topic in CONFIG["topics"]:
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