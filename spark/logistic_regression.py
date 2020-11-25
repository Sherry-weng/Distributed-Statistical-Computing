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

# ArrDelay
air = air.withColumn('Arrdelay', F.when(air["ArrDelay"]>0, 1).otherwise(0))
# air = air.drop('ArrDelay')

# Year
aircount_Year = air.groupBy("Year").count()
aircount_Year = aircount_Year.sort("count",ascending=False)
air = air.withColumn('Year_before1990', F.when(air["Year"] <1990, 1).otherwise(0))
air = air.withColumn('Year_after2000', F.when(air["Year"] >=2000, 1).otherwise(0))
air = air.drop('Year')

# aircount_Year.agg(sum("count")).show()
# result=aircount_Year.withColumn("percent",F.format_number(F.col("count").divide( sum("count").over()).multiply(100),5))
# aircount_Year = aircount_Year.toPandas()
# aircount_Year['count'] = aircount_Year['count'].astype('int')
# list_Year = pd.DataFrame()
# list_Year['name'] = aircount_Year['Year']
# list_Year['count'] = aircount_Year['count'][0]
# list_Year['percent'] = aircount_Year['count'][0]/aircount
# sum = 0
# for i in range(1,len(aircount_Year['count'])):
#     sum = aircount_Year['count'][i]+list_Year.iloc[i-1,1]
#     list_Year.iloc[i,1] = sum
#     list_Year.iloc[i,2] = sum/aircount #求出累计占比

# Month
aircount_Month = air.groupBy("Month").count()
aircount_Month = aircount_Month.sort("count",ascending=False)
spring = [3,4,5]
summer = [6,7,8]
fall = [9,10,11]
air = air.withColumn('Month_spring', F.when(F.col('Month').isin(spring), 1).otherwise(0))
air = air.withColumn('Month_summer', F.when(F.col('Month').isin(summer), 1).otherwise(0))
air = air.withColumn('Month_fall', F.when(F.col('Month').isin(fall), 1).otherwise(0))
air = air.drop('Month')


# aircount_Month = aircount_Month.toPandas()
# list_Month = pd.DataFrame()
# list_Month['name'] = aircount_Month['Month']
# list_Month['count'] = aircount_Month['count'][0]
# list_Month['percent'] = aircount_Month['count'][0]/aircount
# sum = 0
# for i in range(1,len(aircount_Month['count'])):
#     sum = aircount_Month['count'][i]+list_Month.iloc[i-1,1]
#     list_Month.iloc[i,1] = sum
#     list_Month.iloc[i,2] = sum/aircount #求出累计占比

# DayofMonth
# aircount_DayofMonth = air.groupBy("DayofMonth").count()
# aircount_DayofMonth = aircount_DayofMonth.sort("count",ascending=False)
# aircount_DayofMonth = aircount_DayofMonth.toPandas()
# list_DayofMonth = pd.DataFrame()
# list_DayofMonth['name'] = aircount_DayofMonth['DayofMonth']
# list_DayofMonth['count'] = aircount_DayofMonth['count'][0]
# list_DayofMonth['percent'] = aircount_DayofMonth['count'][0]/aircount
# sum = 0
# for i in range(1,len(aircount_DayofMonth['count'])):
#     sum = aircount_DayofMonth['count'][i]+list_DayofMonth.iloc[i-1,1]
#     list_DayofMonth.iloc[i,1] = sum
#     list_DayofMonth.iloc[i,2] = sum/aircount #求出累计占比

# DayofWeek
# aircount_DayofWeek = air.groupBy("DayofWeek").count()
# aircount_DayofWeek = aircount_DayofWeek.sort("count",ascending=False)
# aircount_DayofWeek = aircount_DayofWeek.toPandas()
# list_DayofWeek = pd.DataFrame()
# list_DayofWeek['name'] = aircount_DayofWeek['DayofWeek']
# list_DayofWeek['count'] = aircount_DayofWeek['count'][0]
# list_DayofWeek['percent'] = aircount_DayofWeek['count'][0]/aircount
# sum = 0
# for i in range(1,len(aircount_DayofWeek['count'])):
#     sum = aircount_DayofWeek['count'][i]+list_DayofWeek.iloc[i-1,1]
#     list_DayofWeek.iloc[i,1] = sum
#     list_DayofWeek.iloc[i,2] = sum/aircount #求出累计占比

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
print((air.count(), len(air.columns))) # (5423403, 121)

# 逻辑回归
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

col = air.schema.names
col = col[1:]

air_assembler = VectorAssembler(inputCols=col,outputCol='features')    # 将多列转换为一个向量列
data = air_assembler.transform(air)
data = data.withColumn('label',data['Arrdelay'])
model_data = data.select('label','features')

# 划分训练集/测试集
training_df,test_df=model_data.randomSplit([0.75,0.25])

# 构建和训练模型
log_reg=LogisticRegression(labelCol='label').fit(training_df)
train_results=log_reg.evaluate(training_df).predictions

print("Coefficients: " + str(log_reg.coefficients))
print("Intercept: " + str(log_reg.intercept))

# Coefficients: [0.003535827502987545,0.005412349124322781,0.01306246774409247,
# -0.012369917231556508,-0.0002427916236047132,0.08319559783234988,-0.009766721369756433,
# 0.0,0.0,-0.11500957318925295,0.10317565115987767,-0.15923376310694828,0.04211288730788199,
# 0.37537892058514283,0.09934594462643823,0.2816925492838487,-0.05514577420831161,0.3415082104624351,
# -0.014950495304826342,0.0,-0.05515664740262129,-0.08224438627776738,0.12801845362749917,
# 0.713762679560799,0.6100728785224018,0.6259352571563849,-0.26755254444788323,-0.14925916817790646,
# -0.27168471056917737,-0.11790408225821988,0.7325640153677941,-1.3533744882709977,0.6921310183972331,
# -0.5218713918720382,-1.4116181660709863,-1.2804358471809685,-1.2287895055857143,-0.5892217239300955,
# 0.827590755068008,0.29366178498408874,-0.36420410735901937,-0.19572427754949306,-0.7411775544058071,
# -0.6109845763726484,0.7514477225604727,-0.4957938552465843,-0.5442870734758684,-0.3552515508329727,
# -0.8534689130444059,-1.409584717156125,0.3727033862381912,0.025636728340374434,-0.005136777305416893,
# -0.2375987167424575,0.8421850301328072,-0.18387779061715626,0.6249435207799655,0.8645716260714417,
# -0.5418384773020026,-0.5273058258481245,0.18596220298021124,0.46357037260416445,0.7168764191044232,
# -0.4119170715581932,0.25392468594615325,1.010708797888946,0.15119476797847334,1.0789853045464914,
# 0.5367677148540265,-0.2999332641860222,-0.4809259443941202,-0.24128854209150616,-0.7141249358355197,
# -0.8434241446208702,-0.959540721223348,-0.5012520990006574,-0.6357310526929196,-0.6689140224029895,
# -0.3169315147413789,-0.6592986830524625,-0.738825049361174,-0.273246530483576,-0.8875388037058459,
# -0.02648142860481246,0.31575211487099686,0.047357834400697414,-0.2457133333340691,0.12888879709182224,
# -0.470040355058965,-0.9769465309714666,0.11312895046887395,-0.3699943457330703,0.4179129230989195,
# 0.07577672934365055,-0.657957194827525,0.20373175838122767,-0.0505883995004391,-0.16304436977346795,
# 0.09572010296800625,0.14039625098380845,-0.23816270217664565,-0.3839798452777858,-0.05108937440411501,
# -0.22730388358265008,-0.9367687043054513,-0.2778428131002534,-0.7850126815005721,-0.7324027403110153,
# 0.33778479332753936,0.11801489060144765,0.006486588778008919,0.08273645471681881,-1.0989911225829263,
# -0.6204651257715325,-0.17263247015766492,-0.1660985194629027,-0.14844078628368407,-0.5675063138063441,
# -0.3557458703320527,0.244740623844689]
# Intercept: -3.9931346277381734

# 测试集评估
results=log_reg.evaluate(test_df).predictions

#手动计算混淆矩阵
true_postives = results[(results.label == 1) & (results.prediction == 1)].count()
true_negatives = results[(results.label == 0) & (results.prediction == 0)].count()
false_positives = results[(results.label == 0) & (results.prediction == 1)].count()
false_negatives = results[(results.label == 1) & (results.prediction == 0)].count()

# true_postives：111478
# true_negatives：184088
# false_positives：27277
# false_negatives：58304

# 1.准确率
accuracy=float((true_postives+true_negatives) /(results.count()))
print("accuracy:",accuracy)
# accuracy: 0.775464584530378

# 2.召回率
# 召回率反映了我们能够正确预测出的正类别样本数占正类别观测值总数的比例
recall = float(true_postives)/(true_postives + false_negatives)
print("recall:",recall)
# recall: 0.6565949276130567

# 3.精度
# 精度是指正确预测出的正确正样本数占所有预测的正面观测值总数的比例
precision = float(true_postives) / (true_postives + false_positives)
print("precision:",precision)
# precision: 0.8034160931137617

# lr = LogisticRegression(maxIter=10, regParam=0.3)
# lr_model = lr.fit(data)

