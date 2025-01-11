from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from utils.config import logger
from utils.database import pg_pool, get_pg_connection

logger.debug("Setting up FastAPI app.")

app = FastAPI()

logger.debug("Setting up CORS origins.")

origins = [
    "http://localhost:3000"  # React dev server
]

# Allow CORS
logger.debug("Setting up CORS middleware.")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger.debug("FastAPI app setup complete.")

from routes import aggregated_postgres, double_aggregated_postgres, companies, top_words, top_couples, top_triples, ask_summary, avg_sentiment

@app.get("/debug_pool")
def debug_pool_status():
    if pg_pool is None:
        return {"status": "pg_pool is None"}
    return {"status": "pg_pool is OK"}

app.include_router(aggregated_postgres.router)
app.include_router(double_aggregated_postgres.router)
app.include_router(companies.router)
app.include_router(top_words.router)
app.include_router(top_couples.router)
app.include_router(top_triples.router)
app.include_router(ask_summary.router)
app.include_router(avg_sentiment.router)
    
logger.debug("Routes added to FastAPI app.")
