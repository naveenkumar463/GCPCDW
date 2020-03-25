#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.types import datetime
import sys
import os
import itertools
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import *
import pyspark.sql.functions as func
#Create a Spark session
spark = SparkSession     .builder     .appName("Python Spark SQL basic example")     .getOrCreate()
CONFIG_PATH = os.environ.get('CONFIG_PATH')
print(CONFIG_PATH)


# In[1]:


df_config = spark.read.option("header", "true").csv(CONFIG_PATH+"Config_Student_Success.csv").collect()
SOURCEFILEPATH   = df_config[0]['Std_Success_SourceFile']
TARGETFILEPATH   = df_config[0]['Std_Success_fact_path']
DIMENSIONFILEPATH=df_config[0]['Std_Success_dim_path']
LOOKUPFILEPATH   =df_config[0]['Std_Success_lkp_path']
SEQUENCE_ID = df_config[0]['monotonically_inc_id']

print(SOURCEFILEPATH)
print(TARGETFILEPATH)
print(DIMENSIONFILEPATH)
print(LOOKUPFILEPATH)


# In[18]:


df_tables = spark.read.option("header", "true").csv("gs://sjsu_cdw/Student_Success/Student_Success_Tables.csv").collect()

#print(df_tables)
SourceTablename=[]
#TargetTablename=[]
for x in (range(len(df_tables))):
    a=df_tables[x]['SourceTablename']
    b=df_tables[x]['TargetTablename']
    SourceTablename.append(a)
    #TargetTablename.append(b)
    df_source = spark.read.option("header", "true").option("inferSchema","true").option("dateFormat", "yyyy-dd-mm").option("timestampFormat","dd-MM-yy HH:mm:ss.SSS").option("encoding", "ISO-8859-1").option("nullValue"," ").csv(SOURCEFILEPATH+a+".csv")
    df_source.write.option("header", "true").mode('overwrite').csv(DIMENSIONFILEPATH+b)

