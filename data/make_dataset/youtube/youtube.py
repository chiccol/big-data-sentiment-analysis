from time import sleep
import json
from datetime import datetime
import re

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
  return new_videos, youtube_comapanies_videos

def iso8601_to_seconds(duration):
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)
    if not match:
        return None
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return hours * 3600 + minutes * 60 + seconds

def getcomments_video(video, youtube_scraper, from_date, company, max_num_comments, next_page_token):
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
            print(f"Error fetching comments: {e}")
            return None, comments

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            if datetime.strptime(comment.get("publishedAt", None), date_format) < datetime.strptime(from_date, date_format):
                if flag_pinned_comment:
                    print(f"Pinned comment is older than {from_date}")
                    flag_pinned_comment = False
                    continue
                else:
                    print(f"Comment is older than {from_date}")
                    return None, comments

            extracted_comment = {
                "text": comment.get("textOriginal", None),
                "date": comment.get("publishedAt", None),
                "yt_videoid": video
            }
            comments.append(extracted_comment)
            flag_pinned_comment = False

        num_comments += len(comments)
        next_page_token = response.get('nextPageToken', None)

        if num_comments >= max_num_comments or not next_page_token:
            break

        request = youtube_scraper.commentThreads().list(
            part="snippet",
            videoId=video,
            maxResults=100,
            pageToken=next_page_token
        )
        sleep(5)

    return next_page_token, comments

def fetch_and_store_comments(company_configs, youtube_scraper, output_file="youtube_comments.json"):
    try:
        with open(output_file, 'r') as file:
            stored_comments = json.load(file)
    except FileNotFoundError:
        stored_comments = {}

    for company, config in company_configs.items():
        print(f"Processing company: {company}")
        total_comments = 0
        new_comments = []

        videos, _ = search_videos(
            query=config["query"],
            publishedAfter=config["search_from_date"],
            youtube_scraper=youtube_scraper,
            company=company,
            max_videos=config["max_videos"],
            relevanceLanguage=config["relevance_language"],
            min_duration=config["min_duration"],
            min_comment=config["min_comment"],
            min_view=config["min_view"]
        )

        for video in videos:
            if total_comments >= config["num_comments_to_fetch"]:
                break

            next_page_token = config["videos"].get(video, {}).get("next_page_token", "None")
            next_page_token, comments = getcomments_video(
                video=video,
                youtube_scraper=youtube_scraper,
                from_date=config["get_comments_from_date"],
                company=company,
                max_num_comments=config["max_num_comments_per_scraping"],
                next_page_token=next_page_token
            )

            total_comments += len(comments)
            new_comments.extend(comments)

        stored_comments[company] = stored_comments.get(company, []) + new_comments
        print(f"Fetched {len(new_comments)} comments for {company}.")

    with open(output_file, 'w') as file:
        json.dump(stored_comments, file, indent=4)

    print(f"Comments stored in {output_file}.")
