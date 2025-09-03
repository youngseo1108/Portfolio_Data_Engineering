import sqlalchemy
import pandas as pd
from dotenv import load_dotenv
import os

# load values from .env
load_dotenv()

server = os.getenv('POSTGRES_HOST', 'postgres')
user = os.getenv('POSTGRES_USER', 'postgres')
password = os.getenv('POSTGRES_PASSWORD', 'postgres')
database = os.getenv('POSTGRES_DB', 'perfdb')
port = os.getenv('POSTGRES_PORT', '5432')

connection_string=f'postgresql://{user}:{password}@{server}:{port}/{database}'

engine = sqlalchemy.create_engine(connection_string)
engine.connect()

df = pd.read_parquet('./data/raw/yellow_tripdata_2025-01.parquet').rename(columns={
  'VendorID': 'vendor_id',
  'RatecodeID': 'ratecode_id',
  'PULocationID': 'pu_location_id',
  'DOLocationID': 'do_location_id'})

cols = ['vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'ratecode_id', 'store_and_fwd_flag',
       'pu_location_id', 'do_location_id', 'payment_type', 'fare_amount',
       'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
       'improvement_surcharge', 'total_amount']
df = df[cols]

df.to_sql('taxi', engine, schema='raw', if_exists='replace', index=False)
df.head()