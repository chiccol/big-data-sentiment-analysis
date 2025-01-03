import os
from dotenv import load_dotenv
import logging


def configure_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)

logger = configure_logging()