import json 
import pandas as pd
import csv
import shutil
from tempfile import NamedTemporaryFile
from kafka import KafkaConsumer

# def write_to_csv(Created_at,User_name,Tweet_text):
#     filename='consumer.csv'
#     tempfile=NamedTemporaryFile(mode="w",delete=False)
    
#     fields=['Created_at','User_name','Tweet_text']
    
#     with open(filename, 'r') as csvfile,tempfile:
#         reader=csv.reader(filename,fieldnames=fields)
#         writer=csv.writer(tempfile,fieldnames=fields)
    
#         for row in reader:
#             row['Created_at'],row['User_name'],row['Tweet_text'] =Created_at,User_name,Tweet_text
#             row={'Created_at':row['Created_at'],'User_name':row['User_name'],'Tweet_text':row['Tweet_text']}
#             writer.writerow(row)
#             print("Updated row")
#     shutil.move(tempfile.name,filename)

def write_to_df(df,row,Created_at,User_name,Tweet_text):
    df.loc[row]=Created_at,User_name,Tweet_text
    return df
    
if __name__ == '__main__':
    
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'twitter',
        bootstrap_servers='0.0.0.0:9092',
        auto_offset_reset='latest',
        consumer_timeout_ms=3000,
        group_id='twitter',
    )
    
    df=pd.DataFrame(columns=['Created_at','User_name','Tweet'])
    df_row=0
    for message in consumer:
        consumer.commit()
        record=json.loads(message.value)
        created_at = record['created_at']
        user_name=record['name']
        tweet_text=record['tweet']
        post=dict(Created_at=created_at,Name=user_name,Tweet=tweet_text)
        write_to_df(df,df_row,created_at,user_name,tweet_text)
        print(post)
        df_row+=1

    consumer.close(autocommit=False)
    print(f"Total {df_row} messages received!!!")
    
    df.to_parquet('/home/mhien/Projects/airflow/dags/consumer.parquet',engine='fastparquet')
    print("Transaction completed successfully")
    