import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import pandas as pd
import googleapiclient.discovery

import json
import logging
from datetime import datetime
import re

from typing import Tuple, Optional, List, Any, Dict
from googleapiclient.discovery import Resource
from kafka_producer import KafkaProducer

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("youtube-producer")
logger.info("Started logging")

def encode_message_to_parquet(data: list[dict]) -> bytes: 
    """
    Encodes a list of dictionaries into a in-memory parquet table.
    Args:
        data -> list of dicts
    Returns:
        bytes
    """
    # Infer the schema from the data
    schema = pa.Table.from_pandas(pd.DataFrame(data)).schema

    # Convert the data to an Arrow Table using the inferred schema
    table = pa.Table.from_pandas(pd.DataFrame(data), schema=schema)

    # Write the table to an in-memory bytes buffer as Parquet
    buffer = BytesIO()
    pq.write_table(table, buffer)

    # Return the Parquet bytes for saving or sending
    return buffer.getvalue()

def iso8601_to_seconds(duration: str) -> int:
    """
    Converts an ISO 8601 duration string in the format 'PT#H#M#S' into total seconds.
    Args:
        duration (str): An ISO 8601 duration string.
    Returns:
        int: Total number of seconds. Returns 0 if the duration is "0" or invalid.
    """
    if duration == "0":
        return 0
    # Define a regular expression to extract hours, minutes, and seconds
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)

    if not match:
        return 0  # If the format is invalid

    # Extract hours, minutes, and seconds from the match groups (or 0 if not available)
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    # Convert everything to seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def getcomments_video(
        video: str,
        youtube_scraper: Resource,
        extra_keys: List[str],
        from_date: str,
        company: str,
        max_num_comments: int,
        producer: KafkaProducer,
        next_page_token: Optional[str]
        ) -> Tuple[Optional[str], int, Resource]:
    """
    Fetch comments from a YouTube video, process them, and send them to a Kafka topic.
    Args:
        video (str): The video ID.
        youtube_scraper (googleapiclient.discovery.Resource): The YouTube API scraper resource.
        extra_keys (List[str]): A list of additional YouTube API keys for quota management.
        from_date (str): The starting date in ISO format ("YYYY-MM-DDTHH:MM:SSZ") to fetch comments.
        company (str): The company associated with the video.
        max_num_comments (int): The maximum number of comments to fetch.
        producer (KafkaProducer): The Kafka producer to send the comments to a Kafka topic.
        next_page_token (Optional[str]): The next page token for pagination.
    Returns:
        Tuple[Optional[str], int, Resource]:
            - The next page token if available, otherwise `None`.
            - The total number of comments fetched.
            - The YouTube scraper resource (may change if extra_keys are used).
    Notes:
        - Comments older than `from_date` are skipped.
        - If the quota is exceeded and extra_keys are available, a new key is used.
        - Comments are sent to the Kafka topic in batches encoded as Parquet files.
    """

    request = youtube_scraper.commentThreads().list(
            part="snippet",
            videoId=video,
            maxResults=100,
            pageToken= None if next_page_token == "None" else next_page_token
        )

    bad_requests = 0
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    flag_pinned_comment = True
    num_comments = 0
    comments = []

    while True:
        logger.info(f"Fetching comments for video {video} of company {company}")
        
        if bad_requests > 5:
            return  

        try:
            response = request.execute()
        except Exception as e:
            error_message = str(e)
            if "quotaExceeded" in error_message:
                if extra_keys:
                    youtube_scraper = googleapiclient.discovery.build(
                        "youtube", 
                        "v3", 
                        developerKey=extra_keys[0]
                    )
                    extra_keys.pop(0)
                    logger.info("Using extra key")
                    request = youtube_scraper.commentThreads().list(
                        part="snippet",
                        videoId=video,
                        maxResults=100,
                        pageToken= None if next_page_token == "None" else next_page_token
                        )
                    continue
                logger.error("Quota exceeded. Please try again tomorrow.")
                return next_page_token, num_comments, youtube_scraper
            else:
                bad_requests += 1
                continue
            
        # For each comment in the response
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            if datetime.strptime(comment.get("publishedAt", None), date_format) < datetime.strptime(from_date, date_format):
                if flag_pinned_comment:
                    logger.info(f"Pinned comment is older than {from_date}")
                    flag_pinned_comment = False
                    continue # Skip the pinned comment
                else:
                    logger.info(f"Comment is older than {from_date}")
                    return None, num_comments, youtube_scraper
            extracted_comment = {
                "source": "youtube",
                "text": comment.get("textOriginal", None),
                "date": comment.get("publishedAt", None),
                "company": company,
                "yt_videoid": video,
                "yt_like_count": int(comment.get("likeCount", None)),
                "yt_reply_count": int(item["snippet"].get("totalReplyCount", 0))
            }
            comments.append(extracted_comment)
            flag_pinned_comment = False

        num_comments += len(comments)
        encoded_comments = encode_message_to_parquet(comments)
        logger.info("Sending message to Kafka")
        producer.produce(record = encoded_comments, topic=company)
        # Stop if no more pages or enough comments have been retrieved
        next_page_token = response.get('nextPageToken',None)
        if num_comments >= max_num_comments: 
            logging.info(f"Reached {max_num_comments} comments for video {video} of company {company}")
            return next_page_token, num_comments, youtube_scraper
        if not next_page_token: 
            logging.info(f"No more comments to fetch for video {video} of company {company}")
            return next_page_token, num_comments, youtube_scraper

        request = youtube_scraper.commentThreads().list(
            part="snippet", 
            videoId=video, 
            maxResults=100, 
            pageToken=next_page_token
        )

def search_videos(
        query: str,
        publishedAfter: str,
        youtube_scraper: Resource,
        extra_keys: List[str],
        company: str,
        max_videos: int,
        relevanceLanguage: str = "en",
        min_duration: int = 240,
        min_comment: int = 10,
        min_view: int = 1000
        ) -> Tuple[List[str], Dict[str, Any], Resource]:
    """
    Search for YouTube videos based on a query and time. Then discards the ones that don't meet the minimum views or 
    the minimum comments criteria.
    Args:
        query (str): The search query string.
        publishedAfter (str): Date in ISO format ("YYYY-MM-DDTHH:MM:SSZ") to filter videos published after.
        youtube_scraper (googleapiclient.discovery.Resource): The YouTube API scraper resource.
        extra_keys (List[str]): Additional YouTube API keys for quota management.
        company (str): The company name to associate the videos with.
        max_videos (int): The maximum number of videos to fetch.
        relevanceLanguage (str): The language for video relevance. Defaults to "en".
        min_duration (int): Minimum duration of the video in seconds. Defaults to 240.
        min_comment (int): Minimum number of comments on the video. Defaults to 10.
        min_view (int): Minimum number of views on the video. Defaults to 1000.
    Returns:
        Tuple[List[str], Dict[str, Any], Resource]:
            - A list of video IDs that passed the filters.
            - The updated YouTube companies videos dictionary.
            - The YouTube scraper resource (may change if extra_keys are used).
    Notes:
        - If the quota is exceeded, an extra API key is used (if available).
        - Videos that don't meet criteria are marked with reasons (e.g., "too_short", "currently_irrelevant").
        - New videos are added to the YouTube companies videos dictionary.
    """
    num_videos = 0
    new_videos = []
    youtube_comapanies_videos_path = "companies.json"
    date_format = "%Y-%m-%dT%H:%M:%SZ"

    max_batch_videos = 50
    if max_videos < 50:
        max_batch_videos = max_videos
    
    request_search_videos = youtube_scraper.search().list(
                part="snippet",
                maxResults=max_batch_videos,
                q=query, 
                publishedAfter=publishedAfter,
                relevanceLanguage=relevanceLanguage
            )
    
    while True:
        
        try:
            response_search_videos = request_search_videos.execute()
        except Exception as e:
            error_message = str(e)
            if "quotaExceeded" in error_message:
                if extra_keys:
                    youtube_scraper = googleapiclient.discovery.build(
                        "youtube", 
                        "v3", 
                        developerKey=extra_keys[0]
                    )
                    request_search_videos = youtube_scraper.search().list(
                        part="snippet",
                        maxResults=max_batch_videos,
                        q=query, 
                        publishedAfter=publishedAfter,
                        relevanceLanguage=relevanceLanguage
                        )
                    extra_keys.pop(0)
                    logger.info("Using extra key")
                    continue
                logger.error("Quota exceeded. Please try again tomorrow.")
                return new_videos, youtube_comapanies_videos, None
            else:
                logger.error(f"An error occurred: {error_message}")
                return new_videos, youtube_comapanies_videos, youtube_scraper
            
        next_page_token_search = response_search_videos.get('nextPageToken',None)
        regionCode = response_search_videos['regionCode']
        # Select only the videos e.g. discard playlists
        videoIds = [
            item["id"]["videoId"] for item in response_search_videos["items"]
            if item["id"]["kind"] == "youtube#video"
            ]
        num_videos += len(videoIds)

        with open(youtube_comapanies_videos_path, 'r') as file:
                youtube_comapanies_videos = json.load(file)

        for videoId in videoIds:
                if videoId not in youtube_comapanies_videos[company]["videos"]:
                    logger.info(f"Checking video {videoId}")
                    try:
                        video_info = youtube_scraper.videos().list(part="contentDetails, statistics", id=videoId).execute()
                    except Exception as e:
                        error_message = str(e)
                        if "quotaExceeded" in error_message:
                            if extra_keys:
                                youtube_scraper = googleapiclient.discovery.build(
                                    "youtube", 
                                    "v3", 
                                    developerKey=extra_keys[0]
                                    )
                                video_info = youtube_scraper.videos().list(part="contentDetails, statistics", id=videoId).execute()
                                extra_keys.pop(0)
                                logger.info("Using extra key")
                            else:
                                logger.error("Quota exceeded. Please try again tomorrow.")    
                                return new_videos, youtube_comapanies_videos, None
                        else:
                            logger.error(f"An error occurred: {error_message}")
                            return new_videos, youtube_comapanies_videos, youtube_scraper
                        
                    duration = iso8601_to_seconds(video_info['items'][0]['contentDetails'].get('duration',"0"))
                    view_count = int(video_info['items'][0]["statistics"].get("viewCount", 0))
                    comment_count = int(video_info['items'][0]["statistics"].get("commentCount",0))
                    if duration < min_duration:
                        youtube_comapanies_videos[company]["videos"][videoId] = "too_short"
                        logger.info(f"Video {videoId} is too short")
                        continue
                    if comment_count < min_comment or view_count < min_view:
                        youtube_comapanies_videos[company]["videos"][videoId] = "currently_irrelevant"
                        logger.info(f"Video {videoId} is currently irrelevant") 
                        continue
                    
                    youtube_comapanies_videos[company]["videos"][videoId] = {
                        "date_last_scrape": youtube_comapanies_videos[company]["get_comments_from_date"],
                        "next_page_token" : "None", 
                        "region_code" : regionCode
                    }
                    logger.info(f"Added video {videoId} to {company}")
                    new_videos.append(videoId)
        
        youtube_comapanies_videos[company]["search_from_date"] = datetime.now().strftime(date_format)

        # Save the updated companies videos with the new videos 
        with open(youtube_comapanies_videos_path, 'w') as file:
            json.dump(youtube_comapanies_videos, file, indent=4)

        if not next_page_token_search or num_videos >= max_videos:
            print("No more videos to fetch", flush=True)
            break
        else:
            print(f"Fetching next batch of videos", flush=True)
            max_batch_videos = max_videos - num_videos if max_videos - num_videos < 100 else 100
            request_search_videos = youtube_scraper.search().list(
                part="snippet",
                maxResults=max_batch_videos,
                q=query, 
                publishedAfter=publishedAfter,
                relevanceLanguage=relevanceLanguage,
                pageToken=next_page_token_search
            )
    return new_videos, youtube_comapanies_videos, youtube_scraper