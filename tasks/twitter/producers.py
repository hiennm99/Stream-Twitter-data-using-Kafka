import tweepy
import configparser
import datetime
import time
import json
from pytz import timezone
from kafka import KafkaProducer,KafkaConsumer
from tweepy import Stream, OAuthHandler

# Convert strings to dictionary
def to_dict(created_at_,user_name,text):
    #Convert datetime to string 
    created_at=created_at_.astimezone(timezone('Asia/Bangkok')).strftime("%Y-%m-%d %H:%M:%S")
    
    user_name=f"@{user_name}"
    return dict(created_at=created_at,name=user_name,tweet=text)

# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('ascii')

class StreamListener(tweepy.Stream):

    tweets=0
    limit=10

    def on_connect(self):
        print("You're streaming data from Twitter successfull!!!")

    def on_error(self, status_code):
        print('Error in streaming data from Twitter: %s' % status_code)  

    def on_closed(self,response):
        print("You stopped streaming data from Twitter successfull!!!")

    def on_status(self, status):
        try:   
            message=to_dict(status.created_at,status.user.screen_name,status.text)
            print(message)
            producer.send('twitter',message)
            producer.flush()
            self.tweets+=1
            if self.tweets<self.limit:
                return True 
            else:
                self.disconnect()
                print(f"Total {self.tweets} message sent!!!")
        except Exception as e:
            print(e)
            return False
        return True

    def on_timeout(self,timeout):
        return True

    
if __name__ == '__main__':
    topic='twitter'
    #Kafka Producer
    producer=KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer)

    #Twitter Authentication
    #read configs
    config = configparser.ConfigParser()
    config.read('config.ini')

    api_key=config['twitter']['api_key']
    api_secret=config['twitter']['api_secret']

    access_token=config['twitter']['access_token']
    access_token_secret=config['twitter']['access_token_secret']

    auth = tweepy.OAuthHandler(api_key, api_secret)                            
    api=tweepy.API(auth)

    #Stream Listener
    stream_tweet = StreamListener(api_key,api_secret,access_token,access_token_secret)
    tags=['2022','#trump'] 
    languages=['en']
    stream_tweet.filter(track=tags)