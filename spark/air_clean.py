#! /usr/bin/env python3.6

import findspark
import pandas as pd
import pyspark.sql.functions as F
findspark.init("/usr/lib/spark-current")
#import pyspark
#conf = pyspark.SparkConf().setAppName("My First Spark RDD APP").setMaster("local")  # “yarn”
#sc = pyspark.SparkContext(conf=conf)

import pyspark.sql
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark with DataFrame").getOrCreate()
spark

## Load a local file becasue we are using spark's local mode
air0 = spark.read.options(header='true', inferSchema='true').csv("/data/airdelay_small.csv")
# air0 # the schema is not correct for some variables

# We specify the correct schema by hand
from pyspark.sql.types import *
schema_sdf = StructType([
        StructField('Year', IntegerType(), True),
        StructField('Month', IntegerType(), True),
        StructField('DayofMonth', IntegerType(), True),
        StructField('DayOfWeek', IntegerType(), True),
        StructField('DepTime', DoubleType(), True),
        StructField('CRSDepTime', DoubleType(), True),
        StructField('ArrTime', DoubleType(), True),
        StructField('CRSArrTime', DoubleType(), True),
        StructField('UniqueCarrier', StringType(), True),
        StructField('FlightNum', StringType(), True),
        StructField('TailNum', StringType(), True),
        StructField('ActualElapsedTime', DoubleType(), True),
        StructField('CRSElapsedTime',  DoubleType(), True),
        StructField('AirTime',  DoubleType(), True),
        StructField('ArrDelay',  DoubleType(), True),
        StructField('DepDelay',  DoubleType(), True),
        StructField('Origin', StringType(), True),
        StructField('Dest',  StringType(), True),
        StructField('Distance',  DoubleType(), True),
        StructField('TaxiIn',  DoubleType(), True),
        StructField('TaxiOut',  DoubleType(), True),
        StructField('Cancelled',  IntegerType(), True),
        StructField('CancellationCode',  StringType(), True),
        StructField('Diverted',  IntegerType(), True),
        StructField('CarrierDelay', DoubleType(), True),
        StructField('WeatherDelay',  DoubleType(), True),
        StructField('NASDelay',  DoubleType(), True),
        StructField('SecurityDelay',  DoubleType(), True),
        StructField('LateAircraftDelay',  DoubleType(), True)
    ])

air_newschema = spark.read.options(header='true').schema(schema_sdf).csv("/data/airdelay_small.csv")
air_newschema.describe().show()

# 变量选择
air_with_na = air_newschema.select(['ArrDelay','Year','Month','DayofMonth','DayofWeek',
                            'DepTime','CRSDepTime','CRSArrTime','UniqueCarrier',
                            'ActualElapsedTime','Origin','Dest','Distance'])
# air_with_na.describe().show()
air_with_na.count() # 5548754

# 缺失值清理
air = air_with_na.na.drop()
# air.describe().show()
aircount = air.count() # 5423403  

# Year
aircount_Year = air.groupBy("Year").count()
aircount_Year = aircount_Year.sort("count",ascending=False)
# aircount_Year.agg(sum("count")).show()
# result=aircount_Year.withColumn("percent",F.format_number(F.col("count").divide( sum("count").over()).multiply(100),5))
aircount_Year = aircount_Year.toPandas()
aircount_Year['count'] = aircount_Year['count'].astype('int')
list_Year = pd.DataFrame()
list_Year['name'] = aircount_Year['Year']
list_Year['count'] = aircount_Year['count'][0]
list_Year['percent'] = aircount_Year['count'][0]/aircount
sum = 0
for i in range(1,len(aircount_Year['count'])):
    sum = aircount_Year['count'][i]+list_Year.iloc[i-1,1]
    list_Year.iloc[i,1] = sum
    list_Year.iloc[i,2] = sum/aircount #求出累计占比

# Month
aircount_Month = air.groupBy("Month").count()
aircount_Month = aircount_Month.sort("count",ascending=False)
aircount_Month = aircount_Month.toPandas()
list_Month = pd.DataFrame()
list_Month['name'] = aircount_Month['Month']
list_Month['count'] = aircount_Month['count'][0]
list_Month['percent'] = aircount_Month['count'][0]/aircount
sum = 0
for i in range(1,len(aircount_Month['count'])):
    sum = aircount_Month['count'][i]+list_Month.iloc[i-1,1]
    list_Month.iloc[i,1] = sum
    list_Month.iloc[i,2] = sum/aircount #求出累计占比

# DayofMonth
aircount_DayofMonth = air.groupBy("DayofMonth").count()
aircount_DayofMonth = aircount_DayofMonth.sort("count",ascending=False)
aircount_DayofMonth = aircount_DayofMonth.toPandas()
list_DayofMonth = pd.DataFrame()
list_DayofMonth['name'] = aircount_DayofMonth['DayofMonth']
list_DayofMonth['count'] = aircount_DayofMonth['count'][0]
list_DayofMonth['percent'] = aircount_DayofMonth['count'][0]/aircount
sum = 0
for i in range(1,len(aircount_DayofMonth['count'])):
    sum = aircount_DayofMonth['count'][i]+list_DayofMonth.iloc[i-1,1]
    list_DayofMonth.iloc[i,1] = sum
    list_DayofMonth.iloc[i,2] = sum/aircount #求出累计占比

# DayofWeek
aircount_DayofWeek = air.groupBy("DayofWeek").count()
aircount_DayofWeek = aircount_DayofWeek.sort("count",ascending=False)
aircount_DayofWeek = aircount_DayofWeek.toPandas()
list_DayofWeek = pd.DataFrame()
list_DayofWeek['name'] = aircount_DayofWeek['DayofWeek']
list_DayofWeek['count'] = aircount_DayofWeek['count'][0]
list_DayofWeek['percent'] = aircount_DayofWeek['count'][0]/aircount
sum = 0
for i in range(1,len(aircount_DayofWeek['count'])):
    sum = aircount_DayofWeek['count'][i]+list_DayofWeek.iloc[i-1,1]
    list_DayofWeek.iloc[i,1] = sum
    list_DayofWeek.iloc[i,2] = sum/aircount #求出累计占比

# DepTime

# CRSDepTime

# CRSArrTime

# UniqueCarrier
aircount_UniqueCarrier = air.groupBy("UniqueCarrier").count()
aircount_UniqueCarrier = aircount_UniqueCarrier.sort("count",ascending=False)
aircount_UniqueCarrier = aircount_UniqueCarrier.toPandas()
list_UniqueCarrier = pd.DataFrame()
list_UniqueCarrier['name'] = aircount_UniqueCarrier['UniqueCarrier']
list_UniqueCarrier['count'] = aircount_UniqueCarrier['count'][0]
list_UniqueCarrier['percent'] = aircount_UniqueCarrier['count'][0]/aircount
sum = 0
for i in range(1,len(aircount_UniqueCarrier['count'])):
    sum = aircount_UniqueCarrier['count'][i]+list_UniqueCarrier.iloc[i-1,1]
    list_UniqueCarrier.iloc[i,1] = sum
    list_UniqueCarrier.iloc[i,2] = sum/aircount #求出累计占比

list_UniqueCarrier_names = list_UniqueCarrier.loc[list_UniqueCarrier['percent']<0.81]['name']
for UC_dummy in list_UniqueCarrier_names:
    UC_dummy_name = "UniqueCarrier"+"_"+UC_dummy
    air = air.withColumn(UC_dummy_name, F.when(air["UniqueCarrier"] == UC_dummy,1).otherwise(0))
air = air.drop('UniqueCarrier')
# 前八列达到80%，顺序为：DL/WN/AA/US/UA/NW/CO/TW


# ActualElapsedTime

# Origin
aircount_Origin = air.groupBy("Origin").count()
aircount_Origin = aircount_Origin.sort("count",ascending=False)
aircount_Origin = aircount_Origin.toPandas()
list_Origin = pd.DataFrame()
list_Origin['name'] = aircount_Origin['Origin']
list_Origin['count'] = aircount_Origin['count'][0]
list_Origin['percent'] = aircount_Origin['count'][0]/aircount
sum = 0
for i in range(1,len(aircount_Origin['count'])):
    sum = aircount_Origin['count'][i]+list_Origin.iloc[i-1,1]
    list_Origin.iloc[i,1] = sum
    list_Origin.iloc[i,2] = sum/aircount #求出累计占比

list_Origin_names = list_Origin.loc[list_Origin['percent']<0.81]['name']
for Or_dummy in list_Origin_names:
    Or_dummy_name = "Origin"+"_"+Or_dummy
    air = air.withColumn(Or_dummy_name, F.when(air["Origin"] == Or_dummy,1).otherwise(0))
air = air.drop('Origin')

# Dest
aircount_Dest = air.groupBy("Dest").count()
aircount_Dest = aircount_Dest.sort("count",ascending=False)
aircount_Dest = aircount_Dest.toPandas()
list_Dest = pd.DataFrame()
list_Dest['name'] = aircount_Dest['Dest']
list_Dest['count'] = aircount_Dest['count'][0]
list_Dest['percent'] = aircount_Dest['count'][0]/aircount
sum = 0
for i in range(1,len(aircount_Dest['count'])):
    sum = aircount_Dest['count'][i]+list_Dest.iloc[i-1,1]
    list_Dest.iloc[i,1] = sum
    list_Dest.iloc[i,2] = sum/aircount #求出累计占比

list_Dest_names = list_Dest.loc[list_Dest['percent']<0.81]['name']
for De_dummy in list_Dest_names:
    De_dummy_name = "Dest"+"_"+De_dummy
    air = air.withColumn(De_dummy_name, F.when(air["Dest"] == De_dummy, 1).otherwise(0))
air = air.drop('Dest')

# Distance

air.show()
print((air.count(), len(air.columns))) # (5423403, 118)