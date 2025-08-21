from pathlib import Path
from dotenv import load_dotenv
import os, sys
from pyspark.sql import SparkSession, functions as F

# Load .env
ENV_PATH = Path(__file__).resolve().parents[1] / "config" / ".env"
print(ENV_PATH)
if not ENV_PATH.exists():
  print(f"[Error] .env not found at {ENV_PATH}")
  sys.exit(1)
load_dotenv(ENV_PATH)

# Read environment variables
ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
AK = os.getenv("MINIO_ROOT_USER")
SK = os.getenv("MINIO_ROOT_PASSWORD")
RAW = os.getenv("S3_BUCKET_RAW", "raw")
FEAT = os.getenv("S3_BUCKET_FEAT", "feature")

# Validate values
missing = [k for k,v in {
  "S3_ENDPOINT": ENDPOINT, "MINIO_ROOT_USER": AK, "MINIO_ROOT_PASSWORD": SK
  }.items() if not v]
if missing:
  print(f'[Error] Missing env vars: {missing}. Check config/.env')
  sys.exit(1)

print(f"S3_ENDPOINT={ENDPOINT}")
print(f"MINIO_ROOT_USER={AK if AK else 'Warning: No ROOT_USER'}")
print(f"MINIO_ROOT_PASSWORD={SK if SK else 'Warning: No ROOT_PASSWORD'}")

def spark():
  return (
    SparkSession.builder
      .appName("spark_minio_mvp")
      .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
      .config("spark.hadoop.fs.s3a.access.key", AK)
      .config("spark.hadoop.fs.s3a.secret.key", SK)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
              "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

      # Fix the timeout error (somehow input = string....)
      .config("spark.hadoop.fs.s3a.connection.timeout", "60000")            # 60s
      .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000")  # 10s
      .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")   # 60s -> 60000
      .config("spark.hadoop.fs.s3a.retry.interval", "500")            # 500ms -> 500
      .config("spark.hadoop.fs.s3a.connection.ttl", "300000")         # 5m -> 300000
      .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")  # 24h -> 86400000
      .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")      # 100ms -> 100
      .config("spark.hadoop.fs.s3a.connection.ttl", "300000")            # 5m    -> 5*60*1000 = 300000
      .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800000")  # 30m -> 30*60*1000 = 1800000

      .config("spark.jars.packages",
              "org.apache.hadoop:hadoop-aws:3.3.6,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.367")
      .getOrCreate()
  )


##
def dump_bad_s3a(sp):
    print("=== Non-numeric fs.s3a.* values (likely to explode) ===")
    jconf = sp._jsc.hadoopConfiguration()
    it = jconf.iterator()
    while it.hasNext():
        e = it.next()
        k, v = e.getKey(), e.getValue()
        if k.startswith("fs.s3a."):
            # 숫자만 필요한 키는 다양하지만, 우선 간단히 알파벳이 섞인 값을 표시
            if any(c.isalpha() for c in v):
                print(f"{k} = {v}")
##

def main():
  sp = spark()
  dump_bad_s3a(sp)

  df = sp.read.parquet(f"s3a://{RAW}/nyc_taxi/2024/01/*.parquet")
  clean = (df
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("is_peak",
            (F.col("pickup_hour").between(7,10) | F.col("pickup_hour").between(17,20)).cast("int"))
        .withColumn("passenger_count", F.coalesce(F.col("passenger_count"), F.lit(1)))
        .select("fare_amount","trip_distance","passenger_count","pickup_hour","is_peak"))

  (clean.repartition(4)
        .write.mode("overwrite")
        .partitionBy("pickup_hour")
        .parquet(f"s3a://{FEAT}/nyc_taxi/2024/01/clean/"))

  agg = (clean.groupBy("pickup_hour").agg(
          F.count("*").alias("n"),
          F.round(F.avg("fare_amount"),2).alias("avg_fare"),
          F.round(F.avg("trip_distance"),2).alias("avg_dist")
          )
        )

  (agg.coalesce(1)
      .write.mode("overwrite")
      .parquet(f"s3a://{FEAT}/nyc_taxi/2024/01/agg/"))

  print("saved: s3a://feature/nyc_taxi/2024/01/{clean,agg}")

if __name__ == "__main__":
  main()