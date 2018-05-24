# coding=utf-8
import time,datetime

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, LongType, FloatType

if __name__ == "__main__":


    spark = SparkSession \
        .builder \
        .appName("flight_history") \
        .getOrCreate()


    # Disable log info
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    '''About csv file Flight-History. The CSV files each have six (6) columns containing time, position, and movement 
    information. 

    1) Timestamp - listed Unix Epoch time.
    2) UTC 
    3) Callsign - contains the three-character ICAO airline identifier. 
    4) Position - contains data, reported as latitude and longitude. 
    5) Altitude (in feet)
    6) Speed (Ground Speed in Knots)
    7) Direction
     '''


    df = spark.read.load("data/flight-history/Flight_U22084_(1139cea6).csv",
                         format="csv",
                         sep=",",
                         inferSchema="true",
                         header="true")

    # df.printSchema()


    # Adding UDF column - Добавить колонку конвертирвоания узла в километр
    'Knots (kts) to kilometers/hour (kph)'
    my_func = udf(lambda kts: (kts * 1.852))
    new_col = my_func(df['Speed']).cast(LongType())  # Column instance
    static_df1 = df.withColumn("speed_kph", new_col)

    # static_df1.printSchema()
    # static_df1.show()

    # TODO:Найти время начала полета и добавить в др. DF
    # NOTE: STD stands for ‘Scheduled Time of Departure’
    # NOTE: total flight time

    # std_date_text = '2018-05-18 19:19:17'
    # std_utc = datetime.datetime.strptime(std_date_text, '%Y-%m-%d %H:%M:%S')
    std_timestamp = datetime.datetime.utcfromtimestamp(1525175176)

    # Adding UDF column - Добавить колонку расчета кол-во проведенных часов в полете
    my_func1 = udf(lambda current_timestamp: (
            (datetime.datetime.utcfromtimestamp(current_timestamp) - std_timestamp).total_seconds() / 3600))
    new_col1 = my_func1(static_df1['Timestamp']).cast(LongType())
    static_df2 = static_df1.withColumn("flight_hours", new_col1)

    # static_df2.printSchema()
    # static_df2.show(1000)

    # Adding UDF column - Добавить колонку расчета расстояния
    my_func2 = udf(lambda speed_kph, flight_hours: (speed_kph * flight_hours))
    new_col2 = my_func2(static_df2['speed_kph'], static_df2['flight_hours']).cast(LongType())
    static_df3 = static_df2.withColumn("flown_km", new_col2)

    # static_df3.printSchema()
    # static_df3.show(1000)

    '''1 Case - Добавить колонку расчета средней скорости полета с последнего выхода на связь (поделить расстояние на скорость )'''
    # Adding UDF column - Добавить колонку расчета расстояния
    my_func3 = udf(lambda flight_hours, flown_km: (flown_km / flight_hours) if flown_km > 0 else 0)
    new_col3 = my_func3(static_df3['flight_hours'], static_df3['flown_km']).cast(LongType())
    static_df4 = static_df3.withColumn("avg_speed_kph", new_col3)

    static_df4.printSchema()
    static_df4.show(1000)


    # The flight distance is 3598 ~ 3601 km for Flight_U22084
    # The flight total hours 5h ~ 6h for Flight_U22084


