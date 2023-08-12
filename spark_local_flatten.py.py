import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession  \
	.builder  \
	.master("local[1]") \
	.appName("carrideanalysis")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	
	
# read json data
df = spark.read.json("file:///home/hadoop/cabdata/part-00000-632400b6-0391-492c-9345-6e41b6f20e17-c000.json")

#extracting each columns in json data and store it in dataframe
df1 = df.select(json_tuple("value"),"customer_id","app_version","OS_version","lat","lon","page_id","button_id","is_button_click","is_page_view","is_scroll_up","is_scroll_down","timestamp\n")) \
    .toDF("customer_id","app_version","OS_version","lat","lon","page_id","button_id","is_button_click","is_page_view","is_scroll_up","is_scroll_down","date_timestamp") 
 
#showing records
df1.show()

#write the datahdfs path '/user/capstone/clickstream/' in csv format
df1.coalesce(1).write.format('csv').options(header='true).save('/user/capstone/clickstream/')