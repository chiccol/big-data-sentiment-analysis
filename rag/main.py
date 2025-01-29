import socket
import json

from transformers import AutoModelForCausalLM, AutoTokenizer
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
import torch
from utils import get_reviews, summarizer
import logging
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

# data = {
#     "company": "Apple",
#     "source" : [], # Trustpilot, Youtube and/or Reddit
#     "start_date" : "2021-01-01",
#     "end_date" : "2021-12-31"
# }

def main():
    """
    Main function to retrieve reviews from MongoDB, process them, and generate summaries.
    Connects to MongoDB, retrieves reviews splitted in chnks, summarizes them, and prints the results.
    In the future it should connect to the UI and send the results back.
    """
    logger.info("Waiting for MongoDB to start and to have some data...")
    sleep(30) 
    logger.info("Loading embeddings...")
    embeddings = HuggingFaceEmbeddings(
         model_name = CONFIG["embeddings_model"]
         )
    # MongoDB connection URI (from environment variables or default)
    logger.info("Connecting to MongoDB...")
    client = MongoClient(CONFIG["mongo_uri"])
    # Access the database and collection  
    db_reviews = client[CONFIG["db_name"]]
    rag_collection = db_reviews["rag"]
    # Get the names of all collections in the "reviews" database
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Loading model on {device}")
    tokenizer = AutoTokenizer.from_pretrained(CONFIG["conv_model"])
    # for multiple GPUs install accelerate and do `model = AutoModelForCausalLM.from_pretrained(checkpoint, device_map="auto")`
    model = AutoModelForCausalLM.from_pretrained(CONFIG["conv_model"]).to(device)
    
    for _ in range(CONFIG["connection_attempts"]): 
        try:
            rag_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            rag_socket.bind((CONFIG["RAG_SOCKET_HOST"], CONFIG["RAG_SOCKET_PORT"]))
            rag_socket.listen()
            logger.info("RAG server started")
            break
        except Exception as e:
            logger.error(f"Error starting socket: {e}")
            sleep(5)
    
    else:
        logger.error(f"Could not start the socket after {CONFIG['connection_attempts']} attempts. Exiting...")
        return 0

    logger.info("Waiting for connection...")

    while True:
        
        # Dictionary to store the answers
        answers = {
            "positive": dict(),
            "neutral": dict(),
            "negative": dict()
        }
        conn, addr = rag_socket.accept()
        
        with conn:
            data = conn.recv(1024)
            received_data = json.loads(data.decode("utf-8"))
            logger.info(f"Received data: {received_data}")
            company = received_data["company"]
            sources = received_data["sources"]
            start_date = received_data["start_date"]
            end_date = received_data["end_date"]
            collections = db_reviews.list_collection_names()
            while company not in collections:
                logger.info(f"Company: {company} not found in the database. Waiting for 5 seconds for data to come...")
                sleep(5)
                collections = db_reviews.list_collection_names()
            logger.info(f"Using RAG for company: {company}")
            # Get all reviews for the current company
            logger.info("Retrieving reviews from MongoDB...")
            sleep(10)
            for sentiment in answers:
                logger.info(f"Extracting info for Sentiment: {sentiment}")
                for source in sources:
                    answers[sentiment][source] = dict()
                    logger.info(f"Extracting info for Source: {source}")

                    reviews = get_reviews(db_reviews, sentiment, company, source, start_date, end_date, 
                                          CONFIG["chunk_size"], CONFIG["chunk_overlap"], CONFIG["separator"])
                    # If no reviews are found, add a message to the answers
                    if not reviews: 
                        for topic in CONFIG["topics"]:
                            answers[sentiment][source][topic] = "No reviews found"
                        continue
                    vectorstore = FAISS.from_texts(reviews, embeddings)
                    for topic in CONFIG["topics"]:
                        logger.info(f"Retrieving {sentiment} reviews from {source} source for {company} about {topic}")
                        retrieved_reviews = vectorstore.similarity_search(topic, k=3)
                        retrieved_reviews = [retrieved_review.page_content for retrieved_review in retrieved_reviews]
                        retrieved_reviews = "\n".join(retrieved_reviews)
                        summary = summarizer(retrieved_reviews, sentiment, topic, model, tokenizer, device)
                        answers[sentiment][source][topic] = summary
            rag_collection.update_one(
                {"company": company}, 
                {"$set": {"answers": answers}},
                upsert=True
                    )
            logger.info(f"Answers for {company} have been stored in 'reviews.rag'.")
                    
            # Send the answers back to the client
            conn.sendall("Done".encode("utf-8"))
            logger.info("Answers sent to the client")
        
if __name__ == "__main__":
    main()
