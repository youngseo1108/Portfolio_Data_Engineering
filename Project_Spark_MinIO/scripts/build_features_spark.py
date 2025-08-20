import os
from pyspark.sql import SparkSession, functions as F

EP = os.getenv("S3_ENDPOINT")
AK = os.getenv("MINIO_ROOT_USER")
SK = os.getenv("MINIO_ROOT_PASSWORD")
RAW = os.getenv("S3_BUCKET_RAW","raw")
FEAT = os.getenv("S3_BUCKET_FEAT","feature")

def spark():
  return (SparkSession.builder.appName("spark_minio_mvp")
          .config("spark.hadoop.fs.s3a.endpoint", EP)
          .config("spark.hadoop.fs.s3a.access.key", AK)
          .config("spark.hadoop.fs.s3a.secret.key", SK)
          .config("spark.hadoop.fs.s3a.path.style.access", "true")
          .getOrCreate())

def main():
  sp = spark()
  df = sp.read.parquet(f"s3a://{RAW}/nyc_taxi/2024/01/*.parquet")

  # 핵심 변환/파생
  clean = (df
           .filter(F.col("trip_distance") > 0)
           .filter(F.col("fare_amount") > 0)
           .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
           .withColumn("is_peak", (F.col("pickup_hour").between(7,10) | F.col("pickup_hour").between(17,20)).cast("int"))
           .select("fare_amount","trip_distance","passenger_count","pickup_hour","is_peak"))

  # Aggregation 1: average fare/distance per hour
  agg = (clean.groupBy("pickup_hour")
         .agg(F.count("*").alias("n"),
              F.round(F.avg("fare_amount"),2).alias("avg_fare"),
              F.round(F.avg("trip_distance"),2).alias("avg_dist")))

  # Save the results
  (clean.repartition(4)
        .write.mode("overwrite")
        .partitionBy("pickup_hour")
        .parquet(f"s3a://{FEAT}/nyc_taxi/2024/01/clean/"))

  (agg.coalesce(1)
      .write.mode("overwrite")
      .parquet(f"s3a://{FEAT}/nyc_taxi/2024/01/agg/"))

  print("saved to s3a://feature/nyc_taxi/2024/01/{clean,agg}")


if __name__ == "__main__":
  main()