from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json
import twitter_credentials
import re
import time

topic_name = "olympics"

class StdOutListener(StreamListener):
    def on_data(self, data):
        # try:
        tweet_data = json.loads(data)
        tweet = tweet_data['text']
        if 'RT' not in tweet[:5]:
            tweet = tweet_data['text']
            tweet = clean_tweet(tweet)
            print(tweet)
            producer.send(topic_name, tweet.encode('utf-8'))

        return True

    def on_error(self, status):
        print (status)


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())


producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = StdOutListener()
auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
stream = Stream(auth, l)
stream.filter(languages=["en"], track=["olympics", "Tokyo2020"])
