from time import sleep
import json
from datetime import datetime
import re

import googleapiclient.discovery

from dotenv import load_dotenv
import os
from typing import List, Optional, Tuple, Dict, Any
from googleapiclient.discovery import Resource

    
def search_videos(
        query: str,
        publishedAfter: str,
        youtube_scraper: Resource,
        extra_keys: List[str],
        company: str,
        max_videos: int,
        relevanceLanguage : str = "en",
        min_duration: int = 240,
        min_comment: int = 10,
        min_view: int = 1000,
        next_token_page_search: Optional[str] = None
        ) -> Tuple[List[str], Dict, Optional[str], Optional[Resource]]:
    """
    Search for YouTube videos based on specified criteria.

    Args:
        query (str): Search query string.
        publishedAfter (str): Date in ISO 8601 format (e.g., "YYYY-MM-DDTHH:MM:SSZ").
        youtube_scraper (Resource): YouTube API scraper object.
        extra_keys (List[str]): List of additional API keys to use if quota is exceeded.
        company (str): Name of the company to retrieve the videos for.
        max_videos (int): Maximum number of videos to retrieve.
        relevanceLanguage (str): Language filter for video relevance. Defaults to "en".
        min_duration (int): Minimum video duration in seconds. Defaults to 240 seconds.
        min_comment (int): Minimum number of comments for relevance. Defaults to 10.
        min_view (int): Minimum number of views for relevance. Defaults to 1000.
        next_token_page_search (Optional[str]): Token for paginated search results. Defaults to None.

    Returns:
        Tuple[List[str], dict, Optional[str], Optional[Any]]: 
            - List of new video IDs.
            - Updated dictionary of company videos.
            - Token for the next page of results.
            - Updated YouTube scraper object.
    """
    num_videos = 0
    new_videos = []
    youtube_comapanies_videos_path = "youtube_companies_videos.json"
    date_format = "%Y-%m-%dT%H:%M:%SZ"

    max_batch_videos = 50
    if max_videos < 50: 
        max_batch_videos = max_videos
    
    request_search_videos = youtube_scraper.search().list(
                part="snippet",
                maxResults=max_batch_videos,
                q=query, 
                publishedAfter=publishedAfter,
                relevanceLanguage=relevanceLanguage,
                pageToken=next_token_page_search,
                order="viewCount"
            )

    while True:
        print(f"Looking for videos for {company}")
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
                    print("Using extra key")
                    continue
                print("Quota exceeded. Please try again tomorrow.")
                return new_videos, youtube_comapanies_videos, next_page_token_search, None
            else:
                print(f"An error occurred: {error_message}")
                return new_videos, youtube_comapanies_videos, next_page_token_search, youtube_scraper

        next_page_token_search = response_search_videos.get('nextPageToken',None)
        regionCode = response_search_videos['regionCode']
        videoIds = [
            item["id"]["videoId"] for item in response_search_videos["items"]
            if item["id"]["kind"] == "youtube#video"
            ]
        num_videos += len(videoIds)

        with open(youtube_comapanies_videos_path, 'r') as file:
                youtube_comapanies_videos = json.load(file)

        for videoId in videoIds:
                if videoId not in youtube_comapanies_videos[company]["videos"]:
                    print(f"Checking video {videoId}")
                    try:
                        video_info = youtube_scraper.videos().list(part="contentDetails, statistics", id=videoId).execute()
                    except Exception as e:
                        error_message = str(e)
                        print(error_message)
                        if "quotaExceeded" in error_message:
                            if extra_keys:
                                youtube_scraper = googleapiclient.discovery.build(
                                    "youtube", 
                                    "v3", 
                                    developerKey=extra_keys[0]
                                )
                                print(f"Using extra key")
                                extra_keys.pop(0)
                                video_info = youtube_scraper.videos().list(part="contentDetails, statistics", id=videoId).execute()
                            print("Quota exceeded. Please try again tomorrow.")
                            return "quota exceeded", None, next_page_token_search, None
                        else:
                            print(f"An error occurred: {error_message}")
                            break
                    duration = iso8601_to_seconds(video_info['items'][0]['contentDetails'].get('duration',"0"))
                    view_count = int(video_info['items'][0]["statistics"].get("viewCount",0))
                    comment_count = int(video_info['items'][0]["statistics"].get("commentCount",0))
                    if duration < min_duration:
                        youtube_comapanies_videos[company]["videos"][videoId] = "too_short"
                        print(f"Video {videoId} is too short")
                        continue
                    if comment_count < min_comment or view_count < min_view:
                        youtube_comapanies_videos[company]["videos"][videoId] = "currently_irrelevant"
                        print(f"Video {videoId} is currently irrelevant") 
                        continue
                    
                    youtube_comapanies_videos[company]["videos"][videoId] = {
                        "date_last_scrape": youtube_comapanies_videos[company]["get_comments_from_date"],
                        "next_page_token" : "None", 
                        "region_code" : regionCode
                    }
                    print(f"Added video {videoId} to {company}")
                    new_videos.append(videoId)
        
        youtube_comapanies_videos[company]["search_from_date"] = datetime.now().strftime(date_format)

        with open(youtube_comapanies_videos_path, 'w') as file:
            json.dump(youtube_comapanies_videos, file, indent=4)

        if not next_page_token_search or num_videos >= max_videos:
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
    return new_videos, youtube_comapanies_videos, next_page_token_search, youtube_scraper

def iso8601_to_seconds(duration: str) -> int:
    """
    Converts an ISO 8601 duration string into seconds.
    Args:
        duration (str): ISO 8601 duration string.
    Returns:
        int: Total duration in seconds. Returns 0 if the input is "0" or the format is invalid.
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
        next_page_token: Optional[str]
        ) -> Tuple[Optional[str], List[Dict[str, Any]], Resource]:
    """
    Fetch comments for a specified YouTube video.
    Args:
        video (str): YouTube video ID for which comments are to be fetched.
        youtube_scraper (Resource): YouTube API scraper object.
        extra_keys (List[str]): List of additional API keys for quota handling.
        from_date (str): Only fetch comments posted after this date (ISO 8601 format).
        company (str): Name of the company associated with the video.
        max_num_comments (int): Maximum number of comments to fetch.
        next_page_token (Optional[str]): Token for the next page of comments.
    Returns:
        Tuple[Optional[str], List[Dict[str, Any]], Resource]:
            - Token for the next page of comments (if available).
            - List of fetched comments with metadata.
            - Updated YouTube scraper object.
    """
    # Initialize the request for the YouTube API
    request = youtube_scraper.commentThreads().list(
        part="snippet",
        videoId=video,
        maxResults=100,
        pageToken=None if next_page_token == "None" else next_page_token
    )

    date_format = "%Y-%m-%dT%H:%M:%SZ"
    flag_pinned_comment = True
    num_comments = 0
    comments = []

    while True:
        print(f"Fetching comments for video {video} of company {company}...")

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
                    request = youtube_scraper.commentThreads().list(
                        part="snippet",
                        videoId=video,
                        maxResults=100,
                        pageToken=next_page_token
                        )
                    extra_keys.pop(0)
                    print("Using extra key")
                    continue
                print("Quota exceeded. Please try again tomorrow.")
                return None, comments, None
            else:
                print(f"An error occurred: {error_message}")
                return None, comments, youtube_scraper  

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            published_at = comment.get("publishedAt", None)

             # Stop fetching if comments are older than the specified date
            if datetime.strptime(published_at, date_format) < datetime.strptime(from_date, date_format):
                if flag_pinned_comment:
                    print(f"Pinned comment is older than {from_date}")
                    flag_pinned_comment = False
                    continue
                else:
                    print(f"Comment is older than {from_date}")
                    return None, comments, youtube_scraper

            # Add the comment to the list
            extracted_comment = {
                "text": comment.get("textOriginal", None),
                "date": comment.get("publishedAt", None),
                "yt_videoid": video
            }
            comments.append(extracted_comment)
            flag_pinned_comment = False

        num_comments += len(comments)
        next_page_token = response.get('nextPageToken', None)

        # Stop fetching if the maximum number of comments is reached or no more pages
        if num_comments >= max_num_comments or not next_page_token:
            break

        request = youtube_scraper.commentThreads().list(
            part="snippet",
            videoId=video,
            maxResults=100,
            pageToken=next_page_token
        )

    return next_page_token, comments, youtube_scraper

def fetch_and_store_comments(
        company_configs: Dict[str, Any],
        output_file: str = "youtube_comments.json",
        next_page_token_search: str = None
        ) -> None:
    """
    Fetch and store YouTube comments for multiple companies based on configuration.
    Args:
        company_configs (Dict[str, Any]): Configuration dictionary for each company.
        output_file (str): Path to the output JSON file where comments are stored.
        next_page_token_search (str): Token for the next page of video search results.
    Returns:
        None
    """
    api_service_name = "youtube"
    api_version = "v3"

    # Load API keys from the environment
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "youtube.env"))
    DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")
    DEVELOPER_KEY_2 = os.getenv("DEVELOPER_KEY_2")
    extra_keys = [DEVELOPER_KEY_2]

    # Initialize YouTube API client
    youtube_scraper = googleapiclient.discovery.build(
            api_service_name, 
            api_version, 
            developerKey=DEVELOPER_KEY
        )
    
    # Initialize output file if it doesn't exist
    if not os.path.exists(output_file):
        output_data = {}
        with open(output_file, 'w') as file:
            json.dump(output_data, file, indent=4)

    for company, config in company_configs.items():
        print(f"Processing company: {company}")
        total_comments = 0
        new_comments = []

        while total_comments < config["num_comments_to_fetch"]:
            videos, _, next_page_token_search, youtube_scraper = search_videos(
                query=config["query"],
                publishedAfter=config["search_from_date"] if config["search_from_date"] != "None" else None,
                youtube_scraper=youtube_scraper,
                extra_keys=extra_keys,
                company=company,
                max_videos=config["max_videos"],
                relevanceLanguage=config["relevance_language"],
                min_duration=config["min_duration"],
                min_comment=config["min_comment"],
                min_view=config["min_view"],
                next_token_page_search=next_page_token_search
            )

            if videos == "quota exceeded":
                return  

            for video in videos:
                if total_comments >= config["num_comments_to_fetch"]:
                    print("Fetched enough comments.")
                    break

                next_page_token = config["videos"].get(video, {}).get("next_page_token", "None")
                next_page_token, comments, youtube_scraper = getcomments_video(
                    video=video,
                    youtube_scraper=youtube_scraper,
                    extra_keys=extra_keys,
                    from_date=config["get_comments_from_date"] if config["get_comments_from_date"] != "None" else None,
                    company=company,
                    max_num_comments=config["max_num_comments_per_scraping"],
                    next_page_token=next_page_token
                )

                total_comments += len(comments)
                new_comments.extend(comments)

            print(f"Fetched {len(new_comments)} comments for {company}.")
            if new_comments:
                print("Saving new comments...")
                with open(output_file, 'r+') as file:
                    prev_comments = json.load(file)
                    prev_comments[company] = prev_comments.get(company, []) + new_comments
                    file.seek(0)
                    json.dump(prev_comments, file, indent=4)
                new_comments.clear()

            if next_page_token_search == None:
                print(f"No next page token for company {company}.")
                break
            else:
                print(f"Searching new videos for company {company}")

    print(f"Fetched all requested comments. Comments stored in {output_file}.")