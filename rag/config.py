import os

CONFIG = {
    "conv_model" : "HuggingFaceTB/SmolLM2-360M-Instruct",
    "embeddings_model" : "BAAI/bge-large-en", 
    "RAG_SOCKET_HOST" : "rag",
    "RAG_SOCKET_PORT" : 5000,
    "connection_attempts" : 10,
    "mongo_uri" : os.getenv("MONGO_URI", "mongodb://mongo:27017"),
    "db_name" : "reviews",
    "chunk_size" : 100,
    "chunk_overlap" : 20,
    "separator" : " ",
    "topics" : [
        "Customer service", 
        "Product quality", 
        "Price",
        "General"
    ]
}
