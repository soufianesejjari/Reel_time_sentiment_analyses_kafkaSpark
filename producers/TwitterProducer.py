import asyncio
import twscrape
from twscrape import API, gather
import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime
import time

class TwitterProducer:
    def __init__(self, server, username, password, email, mail_pass=None):
        self.server = server
        self.api = API()
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.server)
        self.topic = 'test-sentiments'
        self.username = username
        self.password = password
        self.email = email
        self.mail_pass = mail_pass

    async def gather_tweets(self, query="morocco", limit=20):
        await self.api.pool.add_account(self.username, self.password, self.email, self.mail_pass)
        await self.api.pool.login_all()

        tweets = await gather(self.api.search(query, limit=limit))

        data = []
        for tweet in tweets:
            tweet_data = {
                'ID': tweet.id,
                'Username': tweet.user.username,
                'Content': tweet.rawContent,
                'Date': tweet.date
            }
            data.append(tweet_data)
            print(tweet.id, tweet.user.username, tweet.rawContent)

        df = pd.DataFrame(data)
        return df

    async def send_tweets_to_kafka(self, df):
        for index, row in df.iterrows():
            message = {
                'source': 'Twitter',
                'date': row['Date'].strftime("%Y-%m-%d %H:%M:%S"),
                'videoId': row['ID'],
                'comment': row['Content'],
                'topic': 'negative',
            }
            self.kafka_producer.send(self.topic, value=json.dumps(message).encode('utf-8'))
            print(f"Sent message: {message}")
            time.sleep(5)

    async def run_async(self, keyword):
        df = await self.gather_tweets(query=keyword)
        await self.send_tweets_to_kafka(df)

    def run(self, keyword):
        if asyncio.get_event_loop().is_running():
            asyncio.create_task(self.run_async(keyword))
        else:
            asyncio.run(self.run_async(keyword))

if __name__ == "__main__":
    server = "localhost:9092"  # replace with your server
    username = "your_twitter_username"  # replace with your Twitter username
    password = "your_twitter_password"  # replace with your Twitter password
    email = "your_email"  # replace with your email
    mail_pass = "your_email_password"  # replace with your email password (optional)
    twitter_keyword = "morocco"  # replace with your keyword

    twitter_producer = TwitterProducer(server, username, password, email, mail_pass)
    twitter_producer.run(twitter_keyword)
