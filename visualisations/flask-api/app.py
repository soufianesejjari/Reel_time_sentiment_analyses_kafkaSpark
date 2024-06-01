from flask import Flask, jsonify
from pymongo import MongoClient
import os

app = Flask(__name__)

# MongoDB connection details
mongo_host = os.environ.get("MONGO_HOST", "localhost")
mongo_port = int(os.environ.get("MONGO_PORT", 27017))
mongo_db = os.environ.get("MONGO_DB", "comment_analysis")

# Connect to MongoDB
try:
    client = MongoClient(mongo_host, mongo_port)
    db = client[mongo_db]
    print(f"Connected to MongoDB at {mongo_host}:{mongo_port}, database: {mongo_db}")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")


@app.route("/")
def home():
    try:
        comments_collection = db.comments
        comments = list(
            comments_collection.find({}, {"_id": 0})
        )  # Exclude the '_id' field
        if comments:
            return jsonify(comments)
        else:
            return jsonify({"message": "No comments found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
