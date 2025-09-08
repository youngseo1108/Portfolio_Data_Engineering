import sqlalchemy
import pandas as pd
from dotenv import load_dotenv
import os, boto3
from io import StringIO, BytesIO

# load values from .env
load_dotenv()

# postgres settings
server = os.getenv('POSTGRES_HOST', 'postgres')
user = os.getenv('POSTGRES_USER', 'postgres')
password = os.getenv('POSTGRES_PASSWORD', 'postgres')
database = os.getenv('POSTGRES_DB', 'perfdb')
port = os.getenv('POSTGRES_PORT', '5432')

connection_string=f'postgresql://{user}:{password}@{server}:{port}/{database}'
engine = sqlalchemy.create_engine(connection_string)
engine.connect()

# minio settings
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('MINIO_ENDPOINT_CONTAINER', 'http://minio:9000'),
    aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'admin'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'supersecret'),
    region_name=os.getenv('MINIO_DEFAULT_REGION', 'us-east-1')
)
bucket = os.getenv('MINIO_BUCKET','raw')

print(">>> reading file...")
obj = s3.get_object(Bucket=bucket, Key='yellow_tripdata_2025-01.parquet')
df = pd.read_parquet(BytesIO(obj['Body'].read())).rename(columns={
                        'VendorID': 'vendor_id',
                        'RatecodeID': 'ratecode_id',
                        'PULocationID': 'pu_location_id',
                        'DOLocationID': 'do_location_id'
                        })

cols = ['vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'ratecode_id', 'store_and_fwd_flag',
       'pu_location_id', 'do_location_id', 'payment_type', 'fare_amount',
       'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
       'improvement_surcharge', 'total_amount']
df = df[cols]

# --- normalise dtypes to match Postgres table ---
# timestamps
df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

# integer columns in raw.taxi
int_cols = [
  "vendor_id", "passenger_count", "ratecode_id",
  "pu_location_id", "do_location_id", "payment_type"
]
for c in int_cols:
  df[c] = pd.to_numeric(df[c], errors="coerce").round().astype("Int64")

# floats / money-like
float_cols = [
  "trip_distance", "fare_amount", "extra", "mta_tax",
  "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"
]
for c in float_cols:
  df[c] = pd.to_numeric(df[c], errors="coerce")

# store_and_fwd_flag as 1-char text; also normalize unexpected values
df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string").str.upper().str.strip()
df.loc[~df["store_and_fwd_flag"].isin(["Y","N"]), "store_and_fwd_flag"] = pd.NA

print(">>> df shape:", df.shape)
print(">>> df head:\n", df.head().to_string(index=False))

print(">>> writing via COPY (fast path)...")
buffer = StringIO()
df.to_csv(buffer, index=False)  # include header
buffer.seek(0)

cols = ",".join([f'"{c}"' for c in df.columns])  # quote col names

conn = engine.raw_connection()
try:
  cur = conn.cursor()
  cur.copy_expert(f'COPY raw.taxi ({cols}) FROM STDIN WITH CSV HEADER', buffer)
  conn.commit()
  cur.close()
finally:
  conn.close()

print(">>> COPY done.")

## METHOD 2: Slow yet straightforward
# print(">>> writing to Postgres (test 1k rows)...")
# sample = df.iloc[:1000].copy()

# try:
#   sample.to_sql('taxi', con=engine, schema='raw', if_exists='append', index=False, method='multi', chunksize=10_000)
#   print(">>> 1k-row test insert OK")
# except Exception as e:
#   print(">>> ERROR during 1k insert:\n", e)
#   traceback.print_exc()
#   raise

# print(">>> writing full dataset...")
# try:
#   df.to_sql('taxi', con=engine, schema='raw', if_exists='append', index=False, method='multi', chunksize=10_000)
#   print(">>> done.")
# except Exception as e:
#   print(">>> ERROR during full insert:\n", e)
#   traceback.print_exc()
#   raise