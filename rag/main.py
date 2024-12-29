from pymongo import MongoClient

from langchain.text_splitter import CharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch 

import os
from time import sleep

def get_reviews(db, sentiment, company):
    comapny_reviews = db[company]
    reviews = comapny_reviews.find({"sentiment": sentiment})
    reviews = [review["text"] for review in reviews]
    text_splitter = CharacterTextSplitter(
        separator=" ",   # Split by spaces
        chunk_size=100,  # Maximum size of each chunk
        chunk_overlap=20  # Overlap between chunks
        )
    splitted_reviews = [text_splitter.split_text(review)[0] for review in reviews]
    return splitted_reviews

def summarizer(text, sentiment, topic, model, tokenizer, device):

    instruction = f"""You are given {sentiment} product reviews about {topic}. 
                  Summarize the complaints in bullet points. Do not simply copy-paste the reviews."""
    messages = [
         {"role": "system", "content":instruction},
         {"role": "user", "content":f"{text}"},
         ]

    with torch.no_grad():   
        input_text = tokenizer.apply_chat_template(messages, tokenize=False)
        inputs = tokenizer.encode_plus(input_text, return_tensors="pt").to(device)
        input_ids = inputs['input_ids']
        attention_mask = inputs['attention_mask']
        # Generate output with the attention mask passed
        outputs = model.generate(
            input_ids, 
            attention_mask=attention_mask, 
            max_new_tokens=100, 
            temperature=0.2, 
            top_p=0.9, 
            do_sample=True
            )
        summary = tokenizer.decode(outputs[0]).split("<|im_start|>assistant")[-1].replace("<|im_end|>", "")
    return summary 

def main():
    print("Wait for mongodb to start and to have some data...", flush=True)
    sleep(30) 
    print("Loading embeddings...", flush=True)
    embeddings = HuggingFaceEmbeddings(
        model_name = "BAAI/bge-large-en"
        )
    # MongoDB connection URI (from environment variables or default)
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
    print("Connecting to MongoDB...", flush=True)
    client = MongoClient(mongo_uri)
    # Access the database and collection
    db_name = "reviews"  # Replace with your database name
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
    checkpoint = "HuggingFaceTB/SmolLM2-360M-Instruct"
    device = "cpu" # for GPU usage or "cpu" for CPU usage
    tokenizer = AutoTokenizer.from_pretrained(checkpoint)
    # for multiple GPUs install accelerate and do `model = AutoModelForCausalLM.from_pretrained(checkpoint, device_map="auto")`
    model = AutoModelForCausalLM.from_pretrained(checkpoint).to(device)
    
    while True:
        print("Retrieving reviews from MongoDB...", flush=True)
        collections = db.list_collection_names() # to be relace with company asking for reviews through UI
        print(f"Got Collections: {collections}", flush=True)
        company = "nordvpn.com"
        # Get all reviews for the current company
        print(f"Company: {company}", flush=True)
        for sentiment in answers:
            print(f"Extracting info for Sentiment: {sentiment}", flush=True)
            reviews = get_reviews(db, sentiment, company)
            if len(reviews) == 0: continue
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