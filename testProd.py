from kafka import KafkaProducer
import json
from datetime import datetime
import time
import random

# Set up Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'test-sentiments'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Video ID for testing
video_id = "test_video_id"

# List of test comments
comments = ['This is a test comment', 'Another test comment', 'Yet another test comment']

while True:
    # Get the current time
    comment_time = datetime.now()

    # Choose a random comment
    comment = random.choice(comments)

    # Create a test message
    message = {
        'source': 'youtube',
        'commentDate': comment_time.strftime("%Y-%m-%d %H:%M:%S"),
        'videoId': video_id,
        'comment': comment
    }

    # Send the message to Kafka
    producer.send(topic, value=json.dumps(message).encode('utf-8'))

    # Print the sent message in the console
    print(f"Sent message: {message}")

    # Wait for 5 seconds before sending the next message
    time.sleep(5)