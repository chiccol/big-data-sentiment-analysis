import torch
from datetime import datetime
from pymongo.database import Database
from typing import List, Optional
from langchain.text_splitter import CharacterTextSplitter
from transformers import AutoModelForCausalLM, AutoTokenizer

def build_query(
    sentiment: str,
    source: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None
) -> dict:
    """
    Build a MongoDB query based on sentiment, source, and optional date range.
    
    Args:
        sentiment: The sentiment to filter by
        source: The source to filter by
        from_date: Optional start date in YYYY-MM-DD format
        to_date: Optional end date in YYYY-MM-DD format
        
    Returns:
        dict: MongoDB query dictionary
    """
    query = {
        "sentiment": sentiment,
        "source": source
    }
    
    if from_date or to_date:
        date_filter = {}
        
        if from_date:
            date_filter["$gte"] = datetime.strptime(from_date, "%Y-%m-%d")
            
        if to_date:
            date_filter["$lte"] = datetime.strptime(to_date, "%Y-%m-%d")
            
        if date_filter:
            query["date"] = date_filter
    
    return query

def get_reviews(
        db: Database, 
        sentiment: str, 
        company: str, 
        source: str,
        from_date: Optional[str],
        to_date: Optional[str],
        chunk_size: int,
        chunk_overlap: int,
        separator: str = " "
        ) -> List[str]:
    """
    Retrieve and preprocess reviews from a MongoDB collection for a specific company and sentiment.
    Args:
        db (Database): MongoDB database object.
        sentiment (str): The sentiment to filter reviews by (e.g., "positive", "neutral", "negative").
        company (str): The name of the company whose reviews are to be fetched.
        source (str): The source of the reviews (e.g., "Trustpilot", "Reddit", "YouTube").
        from_date (str | None): The start date for filtering reviews.
        to_date (str | None): The end date for filtering reviews.
        chunk_size (int): The maximum size of each chunk.
        chunk_overlap (int): The overlap between chunks.
        separator (str): The separator to use for splitting the text.
    Returns:
        List[str]: Preprocessed and chunked reviews.
    """
    sentiment = sentiment.lower()
    query = build_query(sentiment, source, from_date, to_date)
    company_reviews = db[company]
    reviews = company_reviews.find(query)
    reviews = [review["text"] for review in reviews]

    text_splitter = CharacterTextSplitter(
        separator=separator,   # Split by spaces
        chunk_size=chunk_size,  # Maximum size of each chunk
        chunk_overlap=chunk_overlap  # Overlap between chunks
        )
    splitted_reviews = [text_splitter.split_text(review)[0] for review in reviews]
    return splitted_reviews

def summarizer(
    text: str, 
    sentiment: str, 
    topic: str, 
    model: AutoModelForCausalLM, 
    tokenizer: AutoTokenizer, 
    device: str
    ) -> str:
    """
    Summarize reviews into concise bullet points.
    Args:
        text (str): The text to summarize.
        sentiment (str): The sentiment of the reviews (e.g., "positive", "neutral", "negative").
        topic (str): The topic of the reviews (e.g., "Customer service").
        model (PreTrainedModel): The pretrained language model.
        tokenizer (PreTrainedTokenizer): The tokenizer associated with the language model.
        device (str): The device to run inference on ("cpu" or "cuda").
    Returns:
        str: The generated summary.
    """

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
