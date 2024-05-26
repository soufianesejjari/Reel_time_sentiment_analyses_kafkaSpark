import time
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import json
from kafka import KafkaProducer

class YoutubeProducer:
    def __init__(self, server, api_key):
        self.server = server
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self.producer = KafkaProducer(bootstrap_servers=self.server)
        self.topic = 'test-sentiments'
        self.processed_comment_ids = set()

    def scrape_comments(self, video_id,keyword):
        print(f"Scraping comments for video ID: {video_id}")
        request = self.youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            order="time",
            maxResults=5
        )
        response = request.execute()

        for item in response['items']:
            comment_id = item['snippet']['topLevelComment']['id']
            if comment_id not in self.processed_comment_ids:
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comment_time = item['snippet']['topLevelComment']['snippet']['publishedAt']
                comment_time = datetime.strptime(comment_time, "%Y-%m-%dT%H:%M:%SZ")

                one_minute_ago = datetime.utcnow() - timedelta(minutes=5)
                if comment_time >= one_minute_ago:
                    message = {
                        'source': 'youtube',
                        'date': comment_time.strftime("%Y-%m-%d %H:%M:%S"),
                        'videoId': video_id,
                        'comment': comment,
                        'topic': keyword
                    }
                    self.producer.send(self.topic, value=json.dumps(message).encode('utf-8'))
                    print(f"Sent comment: {message}")
                    self.processed_comment_ids.add(comment_id)

    def run(self, video_id,keyword):
        while True:
            self.scrape_comments(video_id,keyword)
            time.sleep(60)

if __name__ == "__main__":
    server = "localhost:9092"  # replace with your server
    api_key = "AIzaSyCUbp2ZcYffR2bHvLvAn1J7QqAWsEjgo3Q"  # replace with your YouTube API key
    video_id = "F6PqxbvOCUI"  # replace with your video ID

*