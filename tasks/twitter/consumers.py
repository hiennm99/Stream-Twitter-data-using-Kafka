import json 
import pandas as pd
import csv
import shutil
from kafka import KafkaConsumer

if __name__ == '__main__':
    
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'test',
        bootstrap_servers='0.0.0.0:9092',
        auto_offset_reset='latest',
        consumer_timeout_ms=3000,
        group_id='test',
    )
    
    df=pd.DataFrame(columns=['Created_at','User_name','Tweet'])
    df_row=0
    results=[]
    for message in consumer:
        mess=[]
        consumer.commit()
        record=json.loads(message.value)
        created_at = record['created_at']
        user_name=record['name']
        tweet_text=record['tweet']
        
        mess.append(created_at)
        mess.append(user_name)
        mess.append(tweet_text)
        results.append(mess)
        
        post=dict(Created_at=created_at,Name=user_name,Tweet=tweet_text)
        print(post)
        df_row+=1
        

    consumer.close(autocommit=False)
    print(f"Total {df_row} messages received!!!")
    df=pd.DataFrame(data=results,columns=['Created_at','User_name','Tweet'])
    df.to_parquet('/home/hiennm/Projects/Airflow/dags/data/Twitter/results.parquet')
    print("Created parquet file successfully")
    