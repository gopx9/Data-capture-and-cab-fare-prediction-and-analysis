import sys
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType,DecimalType,TimestampType
from pyspark.sql.functions import col, unix_timestamp, from_unixtime,date_trunc,count



# Initialize a Spark session
spark = SparkSession.builder.appName("ReadFromHDFS").getOrCreate()

# Define the schema for the dataframe
bookings_schema = StructType([
    StructField("booking_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("customer_app_version", StringType(), True),
    StructField("customer_phone_os_version", StringType(), True),
    StructField("pickup_lat", DecimalType(), True),
    StructField("pickup_lon", DecimalType(), True),
    StructField("drop_lat",DecimalType(), True),
    StructField("drop_lon", DecimalType(), True),
    StructField("pickup_timestamp", TimestampType(), True),
    StructField("drop_timestamp", TimestampType(), True),
    StructField("trip_fare", DecimalType(), True),
    StructField("tip_amount", DecimalType(), True),
    StructField("currency_code", StringType(), True),
    StructField("cab_color", StringType(), True),
    StructField("cab_registration_no", StringType(), True),
    StructField("customer_rating_by_driver", IntegerType(), True),
    StructField("rating_by_customer", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True)
])

# loading the data
car1 = spark.read.load("/user/root/carride/part-m-00000",format = 'csv',header=False, schema= bookings_schema )

# Show the first few rows of the dataframe
car1.show()

car = car1.withColumn("pickup_timestamp", unix_timestamp(col("pickup_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
df = car.groupBy(date_trunc("day", "pickup_timestamp").alias("pickup_date")).agg(count("booking_id").alias("total_bookings"))
df.show()

df = df.orderBy(df['pickup_date'].asc())
df.show()
df.write.mode('overwrite').saveAsTable("car.booking11")
dfdata = spark.read.table("car.booking11")
dfdata.show()
#load the aggregated data into hadoop local folder
datewisebookingsdf.coalesce(1).write.format('csv').options(header='true').save('file:///home/hadoop/datewise_booking/')




