import os
import tweepy
import pymongo
from dotenv import load_dotenv
load_dotenv()


CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")
DB_URL = os.getenv("DB_URL")


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=10, retry_delay=5, retry_errors=5)


client = pymongo.MongoClient(DB_URL)


class Listener(tweepy.StreamListener):
    def on_status(self, status):
        twt = {"id": status.id,
               "tweet": status.text,
               "user": status.user.screen_name}

        db = client.tweetDB
        collection = db.tweets
        collection.insert_one(twt)

    def on_error(self, status_code):
        if status_code == 420:
            return False

class TwitterApp:

    def get_tweets(self):

        listener = Listener()
        stream = tweepy.Stream(auth=api.auth, listener=listener)
        print("Start data streaming")
        stream.filter(track=['bigcompras'], is_async=True)


app = TwitterApp()
app.get_tweets()
