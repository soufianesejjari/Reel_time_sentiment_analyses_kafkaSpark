import time
from googleapiclient.discovery import build
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Set up YouTube Data API credentials
api_key = "AIzaSyAixx4LjLBggdH2lJZHLUljyUgRGWtfznI"
youtube = build('youtube', 'v3', developerKey=api_key)

# Set up Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'webscraping-data'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
last_sent_comment = None  # Initialize the last sent comment

# Scrape real-time comments from YouTube
def scrape_comments():
    global last_sent_comment  # Declare the variable as global
    # Run the scraping function continuously
    video_id = "F6PqxbvOCUI"
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        order="time",
        maxResults=10  # Adjust the number of comments to fetch
    )
    response = request.execute()
# Inside your for loop in the scrape_comments function
   # Inside your for loop in the scrape_comments function
    for item in response['items']:
        comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
        comment_time = item['snippet']['topLevelComment']['snippet']['publishedAt']  # Get the comment's timestamp
        comment_time = datetime.strptime(comment_time, "%Y-%m-%dT%H:%M:%SZ")  # Convert the timestamp to a datetime object

        # Get the current time and subtract one minute
        one_minute_ago = datetime.utcnow() - timedelta(minutes=1)

        # Only send the comment if it was posted in the last minute
        if comment_time >= one_minute_ago:
            message = {
                'source': 'youtube',
                'commentDate': comment_time.strftime("%Y-%m-%d %H:%M:%S"),
                'videoId': video_id,  # Add the video ID to the message
                'comment': comment
            }
            producer.send(topic, value=json.dumps(message).encode('utf-8'))
            print(f"Sent comment: {message}")  # Print the sent comment in the console
while True:
    scrape_comments()
    time.sleep(60)  # Delay for 60 seconds before sending new comments