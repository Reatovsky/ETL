from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("create-table").enableHiveSupport().getOrCreate()

schema = StructType([
    StructField('application_id', StringType(), True),
    StructField('event_time', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('region_code', StringType(), True),
    StructField('product_type', StringType(), True),
    StructField('requested_amount', IntegerType(), True),
    StructField('term_months', IntegerType(), True),
    StructField('credit_score', IntegerType(), True),
    StructField('risk_level', StringType(), True),
    StructField('decision_status', StringType(), True),
    StructField('approved_amount', IntegerType(), True),
    StructField('channel', StringType(), True),
    StructField('employee_review_flag', StringType(), True),
    StructField('processing_time_sec', IntegerType(), True)
])

df = spark.read.option("header", "true").schema(schema).csv("s3a://dz1/Datasets/day_*_dataset.csv")
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3a://dz1/result_table")
spark.stop()