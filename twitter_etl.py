import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs  

def run_twitter_etl():
    access_key = "4QAwNtxYggEDWRfDP8lWtcFpF"
    access_secret = "w0qYr0U7kPeXJVjRdOPBV2aLvfr3ZFBw5e9VWs5t8ia88GmKA6"
    consumer_key = "1596611023573864452-gVFsWcfqOdbI62nkRuzyfY64hXyyPj"
    consumer_secret = "gVEVsgoTla37HjKE56Ohw8NZ51jMAZD4Lt4jyXBcvcbGE"
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAOYcjwEAAAAAOqTY7%2FzjBqFpdZouxA3L1b2DtLg%3DNurAOrF5mLu6VEGicQOf0sNNnTlnonXVZNNfnWOBGAF1f8GXMk"

    #twitter authentication
    # auth = tweepy.OAuthHandler(access_key, access_secret)
    # auth.set_access_token(consumer_key, consumer_secret)

    #create an API object
    # api = tweepy.API(auth)

    # need elevated access to run this
    # tweets = api.user_timeline(screen_name = '@elonmusk',
    #                             count = 200, 
    #                             include_rts = False,
    #                             tweet_mode = 'extended'
    #                             )

    # use Twitter API v2 Client
    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    user = client.get_user(username='nytimes').data
    user_id = user.id
    tweets = client.get_users_tweets(id=user_id, max_results=100)

    ids = []
    for tweet in tweets.data:
        ids.append(str(tweet.id))
    tweets_info = client.get_tweets(ids=ids, tweet_fields=["public_metrics", 'created_at'])

    list=[]
    for tweet in tweets_info.data:
        refined_tweet = {"name":'nytimes',
                        "id": tweet['id'],
                        'text':tweet["text"],
                        'retweet_count':tweet["public_metrics"]['retweet_count'],
                        'like_count':tweet["public_metrics"]['like_count'],
                        'created_at':tweet["created_at"]
                        }
        list.append(refined_tweet)

    #save tweet data into a csv file
    df = pd.DataFrame(list)
    df.to_csv('s3://echo-twitter-airflow-output/refined_tweet.csv')
