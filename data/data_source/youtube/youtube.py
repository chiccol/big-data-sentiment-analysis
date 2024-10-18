from time import sleep
import json
from datetime import datetime

def getcomments_video(video, youtube_scraper, from_date, producer, company):
    """
    Fetch comments from a YouTube video and return them in a DataFrame.
    
    Parameters:
    - video: str of the video id (e.g., "QBGaO89cBMI" from "https://www.youtube.com/watch?v=QBGaO89cBMI")
    - max_comments: int of the maximum number of comments to fetch
    - fields: list of field names to include in the result (e.g., ['publishedAt', 'likeCount', 'textOriginal'])
    - maxBatch: int of the maximum number of comments to fetch per request (must be between 0 and 100)
    
    Returns:
    - DataFrame of requested comments fields
    - Number of bad requests encountered
    """

    request = youtube_scraper.commentThreads().list(
            part="snippet",
            videoId=video,
            maxResults=100
        )

    bad_requests = 0
    date_format = "%Y-%m-%dT%H:%M:%SZ"

    while True:
        # Handle disabled comment sections or failed requests
        try:
            response = request.execute()
        except:
            bad_requests += 1
            continue
        
        # For each comment in the response
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            if datetime.strptime(comment.get("publishedAt", None), date_format) < datetime.strptime(from_date, date_format):
                print("Comment is older than 2021-01-01")
                return 
            extracted_comment = {
                "videoId": video,
                "textOriginal": comment.get("textOriginal", None),
                "likeCount": comment.get("likeCount", None),
                "publishedAt": comment.get("publishedAt", None),
                "totalReplyCount": item["snippet"].get("totalReplyCount", 0)
            }
            extracted_comment_serialized = json.dumps(extracted_comment).encode('utf-8')
            producer.produce(record = extracted_comment_serialized, topic=company)

        # Stop if no more pages or enough comments have been retrieved
        if "nextPageToken" in response:
            nextPageToken = response['nextPageToken']
        else:
            nextPageToken = None
            break
            
        request = youtube_scraper.commentThreads().list(
            part="snippet", 
            videoId=video, 
            maxResults=100, 
            pageToken=nextPageToken
        )

        sleep(5)

def search_videos(query, 
                  publishedAfter,
                  youtube_scraper, 
                  relevanceLanguage = "en",  
                  maxBatch_videos = 50
                    ):
  """
  video: str of the video id, in the url is the field "v=" e.g. "https://www.youtube.com/watch?v=QBGaO89cBMI" -> "QBGaO89cBMI"
  max_comments: int of the maximum number of comments to get
  maxBatch: int of the maximum number of comments to get per request (must be between 0 and 100)
  """
  request_search_videos = youtube_scraper.search().list(
        part="snippet",
        maxResults=maxBatch_videos,
        q=query, 
        publishedAfter=publishedAfter,
        relevanceLanguage=relevanceLanguage
    )
  
  # Execute the request.
  try:
    response_search_videos = request_search_videos.execute()
    nextPageToken_search = response_search_videos['nextPageToken']
    regionCode = response_search_videos['regionCode']

    videoId = [item["id"]["videoId"] for item in response_search_videos['items']]
    return nextPageToken_search, regionCode, videoId
  except Exception as e:
    print(e)