import tweepy
import configparser
import datetime
import time
import json
from pytz import timezone
from kafka import KafkaProducer,KafkaConsumer
from tweepy import Stream, OAuthHandler
import pandas as pd
from tempfile import NamedTemporaryFile

def producer():
    print('------------------------ Start stream tweets from twitter --------------------')

    # Convert strings to dictionary
    def to_dict(created_at_,user_name,text):
        
        #Convert created_at from datetime to string
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

def consumer():
    print('------------------------ Start consume messages from producer --------------------')
    
    #Insert values to a row of dataframe
    def write_to_df(df,row,Created_at,User_name,Tweet_text):
        df.loc[row]=Created_at,User_name,Tweet_text
        return df
    
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'twitter',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        consumer_timeout_ms=3000,
        group_id='twitter',
    )
    
    # Create a Pandas DataFrame
    df=pd.DataFrame(columns=['Created_at','User_name','Tweet'])
    
    df_row=0
    for message in consumer:
        consumer.commit()
        record=json.loads(message.value)
        created_at = record['created_at']
        user_name=record['name']
        tweet_text=record['tweet']
        post=dict(Created_at=created_at,Name=user_name,Tweet=tweet_text)
        
        #Insert record into dataframe
        write_to_df(df,df_row,created_at,user_name,tweet_text)
        print(post)
        df_row+=1

    consumer.close(autocommit=False)
    print(f"Total {df_row} messages received!!!")
    
    #Create a result as parquet file
    df.to_parquet('/home/mhien/Projects/airflow/dags/consumer.parquet',engine='fastparquet')
    print("Transaction completed successfully")
        
if __name__ == '__main__':
    producer()
    time.sleep(3)
    consumer()