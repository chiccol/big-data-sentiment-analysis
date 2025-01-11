import os
import socket
# import threading
from time import sleep

# from transformers import AutoModelForCausalLM, AutoTokenizer
# from langchain.vectorstores import FAISS
# from langchain.embeddings import HuggingFaceEmbeddings
import torch
# from utils import get_reviews, summarizer
import logging
from pymongo import MongoClient

# logging 
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

model = "HuggingFaceTB/SmolLM2-360M-Instruct"
embeddings_model = "BAAI/bge-large-en"
device = "cuda" if torch.cuda.is_available() else "cpu" 
mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
db_name = "reviews"
logger.info(f"Device: {device}")

RAG_SOCKET_HOST = "rag"
RAG_SOCKET_PORT = 5000

def main():
    """
    Main function to retrieve reviews from MongoDB, process them, and generate summaries.
    Connects to MongoDB, retrieves reviews splitted in chnks, summarizes them, and prints the results.
    In the future it should connect to the UI and send the results back.
    """
    logger.info("Waiting for MongoDB to start and to have some data...")
    sleep(30) 
    logger.info("Loading embeddings...")
    # embeddings = HuggingFaceEmbeddings(
    #     model_name = embeddings_model
    #     )
    # MongoDB connection URI (from environment variables or default)
    logger.info("Connecting to MongoDB...")
    client = MongoClient(mongo_uri)
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
    logger.info("Loading model...")
    # tokenizer = AutoTokenizer.from_pretrained(model)
    # # for multiple GPUs install accelerate and do `model = AutoModelForCausalLM.from_pretrained(checkpoint, device_map="auto")`
    # model = AutoModelForCausalLM.from_pretrained(model).to(device)
    
    # just for testing I send the message "BELLA" to the client
    messaggio_super_segreto = "Done"
    
    while True:
        
        try:
            rag_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            rag_socket.bind((RAG_SOCKET_HOST, RAG_SOCKET_PORT))
            rag_socket.listen()
            conn, addr = rag_socket.accept()
            sleep(10)
            
            with conn:
                logger.info(f"Connected by {addr}")
                data = conn.recv(1024)
                if not data:
                    continue
                
                logger.info(f"Received data: {data}")
            
                # # print("Retrieving reviews from MongoDB...", flush=True)
                # logger.info("Retrieving reviews from MongoDB...")
                # collections = db.list_collection_names()
                # # print(f"Got Collections: {collections}", flush=True)
                # logger.info(f"Got Collections: {collections}")
                
                # company = data.decode("utf-8")

                # while company not in collections:
                #     # print(f"Company: {company} not found in the database. Waiting for 5 seconds for data to come...", flush=True)
                #     logger.info(f"Company: {company} not found in the database. Waiting for 10 seconds for data to come...")
                #     sleep(10)
                #     collections = db.list_collection_names()
                # # Get all reviews for the current company
                # # print(f"Company: {company}", flush=True)
                # logger.info(f"Company: {company}")
                # for sentiment in answers:
                #     # print(f"Extracting info for Sentiment: {sentiment}", flush=True)
                #     logger.info(f"Extracting info for Sentiment: {sentiment}")
                #     reviews = get_reviews(db, sentiment, company)
                #     if len(reviews) == 0: 
                #         for topic in topics:
                #             answers[sentiment][topic] = "No reviews found"
                #         continue
                #     vectorstore = FAISS.from_texts(reviews, embeddings)
                #     for topic in topics:
                #         # print(f"Topic: {topic}", flush=True)
                #         logger.info(f"Topic: {topic}")
                #         retrieved_reviews = vectorstore.similarity_search(topic, k=3)
                #         retrieved_reviews = [retrieved_review.page_content for retrieved_review in retrieved_reviews]
                #         retrieved_reviews = "\n".join(retrieved_reviews)
                #         summary = summarizer(retrieved_reviews, sentiment, topic, model, tokenizer, device)
                #         answers[sentiment][topic] = summary
                #         # print("Summary:", answers[sentiment][topic], flush=True)
                #     # print("-"*50, flush=True)
                #     logger.info("-"*50)
                    
                #     conn.sendall("Done".encode("utf-8"))
                
                conn.sendall(messaggio_super_segreto.encode("utf-8"))
                
                    
        except Exception as e:
            logger.info(f"Client not connected: {e}")
            sleep(10)
            continue

if __name__ == "__main__":
    main()