from time import sleep
import json
from datetime import datetime
import re

def iso8601_to_seconds(duration):
    # Define a regular expression to extract hours, minutes, and seconds
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)

    if not match:
        return None  # If the format is invalid

    # Extract hours, minutes, and seconds from the match groups (or 0 if not available)
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    # Convert everything to seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def getcomments_video(video, youtube_scraper, from_date, company, max_num_comments, producer, next_page_token):
    """
    Fetch comments from a YouTube video and return them in a DataFrame.
    
    Parameters:
    - video: str, the video ID
    - youtube_scraper: googleapiclient.discovery.Resource, the YouTube API scraper
    - from_date: str, the date from which to start fetching comments
    - producer: KafkaProducer, the Kafka producer to send the comments to a Kafka topic
    - 
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

    while True:
        print(f"Fetching comments for video {video} of company {company}")
        # Handle disabled comment sections or failed requests
        try:
            response = request.execute()
        except:
            bad_requests += 1
            continue

        comments = []
        # For each comment in the response
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            if datetime.strptime(comment.get("publishedAt", None), date_format) < datetime.strptime(from_date, date_format):
                if flag_pinned_comment:
                    print(f"Pinned comment is older than {from_date}")
                    flag_pinned_comment = False
                    continue # Skip the pinned comment
                else:
                    print(f"Comment is older than {from_date}")
                    return 
            extracted_comment = {
                "source": "youtube",
                "text": comment.get("textOriginal", None),
                "date": comment.get("publishedAt", None),
                "yt-videoId": video,
                "yt-like-count": comment.get("likeCount", None),
                "yt-reply-count": item["snippet"].get("totalReplyCount", 0)
            }
            comments.append(extracted_comment)
            flag_pinned_comment = False

        comments = json.dumps(comments).encode('utf-8')
        num_comments += len(comments)
        print(comments)
        producer.produce(record = comments, topic=company)

        # Stop if no more pages or enough comments have been retrieved
        next_page_token = response.get('nextPageToken',None)
        if num_comments >= max_num_comments: 
            print(f"Reached {max_num_comments} comments for video {video} of company {company}")
            return next_page_token
        if not next_page_token: 
            print(f"No more comments to fetch for video {video} of company {company}") 
            return None

        request = youtube_scraper.commentThreads().list(
            part="snippet", 
            videoId=video, 
            maxResults=100, 
            pageToken=next_page_token
        )

        sleep(5)

def search_videos(query, 
                  publishedAfter,
                  youtube_scraper, 
                  company,
                  max_videos,
                  relevanceLanguage = "en",
                  min_duration = 240,
                  min_comment = 10,
                  min_view = 1000
                    ):
  """
  query: str of the search query
  publishedAfter: str of the date in the format "YYYY-MM-DDTHH:MM:SSZ"
  company: str of the company name which is retrieved from youtube_companies_videos.json
  max_videos: int of the maximum number of videos to get
  relevanceLanguage: str of the language of the videos
  min_duration: int of the minimum duration of the video in seconds
  min_comment: int of the minimum number of comments
  min_view: int of the minimum number of views
  max_comments: int of the maximum number of comments to get
  """
  num_videos = 0
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
            relevanceLanguage=relevanceLanguage
        )
  
  while True:
    
    response_search_videos = request_search_videos.execute()
    next_page_token_search = response_search_videos.get('nextPageToken',None)
    regionCode = response_search_videos['regionCode']
    videoIds = [item["id"]["videoId"] for item in response_search_videos['items']]
    num_videos += len(videoIds)
    new_videos = []

    with open(youtube_comapanies_videos_path, 'r') as file:
            youtube_comapanies_videos = json.load(file)

    for videoId in videoIds:
            if videoId not in youtube_comapanies_videos[company]["videos"]:
                print(f"Checking video {videoId}")
                video_info = youtube_scraper.videos().list(part="contentDetails, statistics", id=videoId).execute()
                duration = iso8601_to_seconds(video_info['items'][0]['contentDetails']['duration'])
                view_count = int(video_info['items'][0]["statistics"]["viewCount"])
                comment_count = int(video_info['items'][0]["statistics"]["commentCount"])
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

    print("Saving file...")
    with open(youtube_comapanies_videos_path, 'w') as file:
        json.dump(youtube_comapanies_videos, file, indent=4)

    if not next_page_token_search or num_videos >= max_videos:
        print("No more videos to fetch")
        break
    else:
        print(f"Fetching next batch of videos")
        max_batch_videos = max_videos - num_videos if max_videos - num_videos < 100 else 100
        request_search_videos = youtube_scraper.search().list(
            part="snippet",
            maxResults=max_batch_videos,
            q=query, 
            publishedAfter=publishedAfter,
            relevanceLanguage=relevanceLanguage,
            pageToken=next_page_token_search
        )
  return new_videos, youtube_comapanies_videos
