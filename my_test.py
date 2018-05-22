from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

import os

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 pyspark-shell'
if __name__ == "__main__":
    bootstrapServers = "localhost:9092"
    subscribeType = "subscribe"
    topics = "Cads"

    spark = SparkSession \
        .builder \
        .appName("my_test") \
        .getOrCreate()


    # Disable log info
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Create Dataframe representing the stream of input json from kafka
    rawKafkaDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option(subscribeType, topics) \
        .load() \
        .selectExpr("CAST(value AS STRING)", "CAST(timestamp as STRING)")




    rawKafkaDF.printSchema()

    # Convert to JSON and then parse json value
    cadsJsonSchema = StructType() \
        .add("EventName", StringType(), False) \
        .add("EventTime", TimestampType(), False) \
        .add("Value", IntegerType(), False, )

    json_df = rawKafkaDF \
        .select(from_json("value", cadsJsonSchema) \
                .alias("parsed_json_values"), rawKafkaDF['timestamp'].cast(TimestampType()) \
                .alias("KafkaTimestamp"))

    json_df.printSchema()

    message_data = json_df.select("parsed_json_values.*", "KafkaTimestamp")

    message_data.printSchema()

    # Adding UDF column
    my_func = udf(lambda event_time, kafka_timestamp: (kafka_timestamp - event_time))
    new_col = my_func(message_data['EventTime'], message_data['KafkaTimestamp'])  # Column instance

    message_data1 = message_data.withColumn("new_col", new_col)

    # Group the data by window and word and compute the count of each group
    # message_data2 = message_data1 \
    #     .where("EventName=A")

    # Select the A events
    # message_data2 = message_data1.select("EventName", "EventTime", "Value", "KafkaTimestamp", "new_col").where("EventName = 'A'")

    # message_data3 = message_data1 \
    #     .groupBy(window(message_data1.EventTime, "2 seconds", "1 seconds"), message_data1.EventName).count

    # Running count of the number of updates for each event type
    message_data2 = message_data1.groupBy(
        "EventName"
    ).agg(max("Value"), sum("Value").alias("python fucks"), count("*").alias("event count"))


    # Start running the query that prints the running counts to the console


    # .trigger(processingTime='1 seconds') \

    # query2 = message_data2 \
    #     .writeStream \
    #     .outputMode("complete") \
    #     .format("json") \
    #     .option("path", "temp/destination") \
    #     .option("checkpointLocation", "temp/checkpointLocation") \
    #     .start()

    # query1 = message_data2 \
    #     .writeStream \
    #     .queryName("my_test") \
    #     .outputMode("complete") \
    #     .option("checkpointLocation", "temp/checkpointLocation") \
    #     .format("memory") \
    #     .start()

    # df = spark.sql("select * from my_test").show()
    # print ("Is streaming: " + df.isStreaming())
    query = message_data2 \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "temp/checkpointLocation") \
        .option("truncate", False) \
        .start()



    # query2.awaitTermination()
    # query1.awaitTermination()
    query.awaitTermination()
