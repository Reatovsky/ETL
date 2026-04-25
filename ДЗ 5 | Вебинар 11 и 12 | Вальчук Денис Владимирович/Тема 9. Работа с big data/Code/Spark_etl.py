from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

spark = SparkSession.builder.appName("Parking Data Transformation")\
    .config("spark.sql.adaptive.enabled", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

input_path = sys.argv[1]
output_path = "s3a://dz1/parking_output/"


def read_data(spark, path):
    df = spark.read.parquet(path)
    df = df.withColumn("hour", hour(to_timestamp("start_time")))
    
    return df

def transform_data(df):
    cleaned_df = df.filter(col("final_cost").isNotNull() & (col("duration_minutes") > 0))
    
    revenue_by_zone = cleaned_df.groupBy("parking_zone_id", "parking_zone_name")\
                                .agg(
                                    count("*").alias("total_transactions"),
                                    sum("final_cost").alias("total_revenue"),
                                    round(avg("final_cost"), 2).alias("avg_revenue")
                                ).orderBy(col("total_revenue").desc())
    
    hourly_stats = cleaned_df.groupBy("parking_zone_id", "parking_zone_name", "hour")\
                            .agg(
                                count("*").alias("transactions_count"),
                                round(sum("final_cost"), 2).alias("revenue")
                            ).orderBy("parking_zone_id", "hour")
    
    day_night_stats = cleaned_df.withColumn("period", when((col("hour") >= 8) & (col("hour") < 20), "day")\
                                .otherwise("night")).groupBy("parking_zone_id", "parking_zone_name", "period")\
                                .agg(
                                    count("*").alias("transactions"),
                                    round(sum("final_cost"), 2).alias("revenue")
                                )
    
    return revenue_by_zone, hourly_stats, day_night_stats

def write_outputs(revenue_by_zone, hourly_stats, day_night_stats, output_path):
    write_options = {
        "mode": "overwrite",
        "header": "true",
        "encoding": "UTF-8",
        "quoteAll": "true"
    }
    
    output_path_revenue = f"{output_path}/revenue_by_zone"
    revenue_by_zone.write\
                    .mode(write_options["mode"])\
                    .option("header", write_options["header"])\
                    .option("encoding", write_options["encoding"])\
                    .option("quoteAll", write_options["quoteAll"])\
                    .csv(output_path_revenue)
 
    
    output_path_hourly = f"{output_path}/hourly_stats"
    hourly_stats.write\
                .mode(write_options["mode"])\
                .option("header", write_options["header"])\
                .option("encoding", write_options["encoding"])\
                .option("quoteAll", write_options["quoteAll"])\
                .csv(output_path_hourly)
    

    output_path_daynight = f"{output_path}/day_night_stats"
    day_night_stats.write\
                    .mode(write_options["mode"])\
                    .option("header", write_options["header"])\
                    .option("encoding", write_options["encoding"])\
                    .option("quoteAll", write_options["quoteAll"])\
                    .csv(output_path_daynight)



def main():
    try:
        raw_df = read_data(spark, input_path)
        revenue_by_zone, hourly_stats, day_night_stats = transform_data(raw_df)

        write_outputs(revenue_by_zone, hourly_stats, day_night_stats, output_path)
        
        
    except Exception as e:
        print(f"\nERROR: {e}")

        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

main()