import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from pyspark.sql.functions import udf
import datetime

# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])


def run_spark_job(spark):
    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port


    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', '0.0.0.0:9092') \
        .option('subscribe', 'sf-data') \
        .option('startingOffsets', 'earliest') \
        .option('maxRatePerPartition', 1000) \
        .option('maxOffsetsPerTrigger', 100) \
        .option('stopGracefullyOnShutdown', 'true') \
        .load()



    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    get_hour = udf(lambda x: x.hour)

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name", "disposition", 'call_date_time')\
        .withWatermark("call_date_time", "60 minutes")

    hour_table = distinct_table.withColumn("hour", get_hour(distinct_table.call_date_time))
    # count the number of original crime type
    agg_df = hour_table.groupBy(hour_table.original_crime_type_name, hour_table.hour).count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df.writeStream \
        .queryName('sf-crime-data')\
        .outputMode("complete") \
        .format("console") \
        .start()

    # TODO attach a ProgressReporter
    #query.awaitTermination()
    #
    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)
    #
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    #
    # # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    radio_code_df.printSchema()
    #
    # # TODO join on disposition column
    join_query = distinct_table \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .queryName("join-sf-kafka") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .config('spark.executor.memory', '1g') \
        .config('spark.driver.memory', '4g') \
        .config('spark.default.parallelism', '8') \
        .config('spark.sql.shuffle.partitions', '8') \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()


    logger.info("Spark started")
    run_spark_job(spark)
    spark.stop()
