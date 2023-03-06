import sqlalchemy 
import pandas as pd
from datetime import datetime

mysql_user='root',
mysql_password='123456'
mysql_host='localhost'
mysql_port='3306'
mysql_db='airflow'
# mysql_engine=sqlalchemy.create_engine(f'mysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')
# conn=mysql_engine.connect()
df=pd.read_parquet('consumer.parquet')
print(type(df['Created_at']))
print(df)