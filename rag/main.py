import socket

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

data = {
    "company": "Apple",
    "source" : [], # Trustpilot, Youtube and/or Reddit
    "start_date" : "2021-01-01",
    "end_date" : "2021-12-31"
}

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
    db_reviews = client[CONFIG["db_name"]]
    rag_collection = db_reviews["rag"]
    # Get the names of all collections in the "reviews" database
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Loading model on {device}", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(CONFIG["conv_model"])
    # for multiple GPUs install accelerate and do `model = AutoModelForCausalLM.from_pretrained(checkpoint, device_map="auto")`
    model = AutoModelForCausalLM.from_pretrained(CONFIG["conv_model"]).to(device)
    logger.info("Generation model:", CONFIG["conv_model"], flush=True)
    logger.info("Embeddings model:", CONFIG["embeddings_model"], flush=True)
    logger.info("Retrieving by the following topics:", CONFIG["topics"], flush=True)

    logger.info(f"Starting server on {CONFIG['RAG_SOCKET_HOST']}:{CONFIG['RAG_SOCKET_PORT']}", flush=True)
    for _ in range(CONFIG["connection_attempts"]): 
        try:
            rag_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            rag_socket.bind((CONFIG["RAG_SOCKET_HOST"], CONFIG["RAG_SOCKET_PORT"]))
            rag_socket.listen()
            logger.info("RAG server started", flush=True)
            break
        except Exception as e:
            logger.error(f"Error starting socket: {e}", flush=True)
            sleep(5)
    
    if rag_socket not in locals():
        logger.error(f"Could not start the socket after {CONFIG['connection_attempts']} attempts. Exiting...", flush=True)
        return 0

    while True:
        
        # Dictionary to store the answers
        answers = {
            "positive": dict(),
            "neutral": dict(),
            "negative": dict()
        }
        
        try:
            conn, addr = rag_socket.accept()
            logger.info(f"Connected by {addr}", flush=True)
        except:
            continue
        
        with conn:
            data = conn.recv(1024)
            company = data.decode("utf-8") 
            collections = db_reviews.list_collection_names()
            while company not in collections:
                logger.info(f"Company: {company} not found in the database. Waiting for 5 seconds for data to come...", flush=True)
                sleep(5)
                collections = db_reviews.list_collection_names()
            logger.info(f"Using RAG for company: {company}", flush=True)
            # Get all reviews for the current company
            logger.info("Retrieving reviews from MongoDB...", flush=True)
            for sentiment in answers:
                logger.info(f"Extracting info for Sentiment: {sentiment}", flush=True)
                reviews = get_reviews(db_reviews, sentiment, company, CONFIG["chunk_size"], CONFIG["chunk_overlap"], CONFIG["separator"])
                # If no reviews are found, add a message to the answers
                if not reviews: 
                    for topic in CONFIG["topics"]:
                        answers[sentiment][topic] = "No reviews found"
                    continue
                vectorstore = FAISS.from_texts(reviews, embeddings)
                for topic in CONFIG["topics"]:
                    logger.info(f"Retrieving {sentiment} reviews for {company} about {topic}", flush=True)
                    retrieved_reviews = vectorstore.similarity_search(topic, k=3)
                    retrieved_reviews = [retrieved_review.page_content for retrieved_review in retrieved_reviews]
                    retrieved_reviews = "\n".join(retrieved_reviews)
                    summary = summarizer(retrieved_reviews, sentiment, topic, model, tokenizer, device)
                    answers[sentiment][topic] = summary
                    logger.info("Summary:", answers[sentiment][topic], flush=True)
                rag_collection.update_one(
                    {"_id": company}, 
                    {"$set": {"answers": answers}},
                    upsert=True
                )
                logger.info(f"Answers for {company} have been stored in 'reviews.rag' with _id '{company}'.")
                
                # Send the answers back to the client
                conn.sendall("Done".encode("utf-8"))
        
if __name__ == "__main__":
    main()