# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3f7b9ea2-6658-477c-96f1-1c32ac7b0bfc",
# META       "default_lakehouse_name": "landingOraData",
# META       "default_lakehouse_workspace_id": "24d3494f-c139-487d-b721-61ed4231cfc4"
# META     },
# META     "environment": {
# META       "environmentId": "bb193d2c-0f24-4497-851d-05af58ebe92b",
# META       "workspaceId": "24d3494f-c139-487d-b721-61ed4231cfc4"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Notebook to conect to Oracle 19 under **VPN** with no public access 


# MARKDOWN ********************

# Download JDBC driver from --> https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

# CELL ********************

from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Testing conectivity to ip

# CELL ********************

from tcppinglib import tcpping 
host= tcpping( '4.223.225.126' , 1521, interval =3,timeout=2) 
host.is_alive 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder \
    .appName("Iniciando com Spark") \
    .config("spark.driver.extraClassPath", "ojdbc11.jar") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

properties = {
    'driver': 'oracle.jdbc.driver.OracleDriver',
    'url': 'jdbc:oracle:thin:@4.223.225.126:1521/oratest1',
    'user': "System",
    'password': "OraPasswd1",
    'dbtable': 'Sales1'
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.format('jdbc').option('driver', properties['driver']).option('url', properties['url']).option('user', properties['user']).option('password', properties['password']).option('dbtable', properties['dbtable']).load()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#executing on Iberdrola env 
# url = "jdbc:oracle:thin:@//host:1521/db"
url= 'jdbc:oracle:thin:@4.223.225.126:1521/oratest1' 
user ='system'
password ='OraPasswd1'
#driver = 'oracle.jdbc.driver.0racleDriver'
driver = 'oracle.jdbc.driver.OracleDriver'

sql='(select * from Sales1)' # with dbtable
#print(int(jdbcDF.collect()[0][0]))
#countrows='(select count(*) from PEOPLE2)' # with dbtable
#jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable',sql).option('user',user).option('password',password).option('partitionColumn','ID').option('numpartitions','10').option("lowerBound", '0').option("UpperBound", "8000000").option('driver',driver).load()
#jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable','PEOPLE2').option('user',user).option('password',password).option('driver',driver).load()
#jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable',countrows).option('user',user).option('password',password).option('driver',driver).load()
#numPartitions = jdbcDF.rdd.getNumPartitions()
#countquery='select count(*) from PEOPLE2'
jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable',sql).option('user',user).option('password',password).option('driver',driver).load()
#jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable',sql).option('user',user).option('password',password).option('partitionColumn','ID').option('numpartitions','10').option("lowerBound", '0').option("UpperBound", "8000000").option('driver',driver).load()
numPartitions = jdbcDF.rdd.getNumPartitions()
print(f"Number of Partition -> {numPartitions}")
display(jdbcDF.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

numPartitions = jdbcDF.rdd.getNumPartitions()
display(numPartitions)
bytesPartitions = spark.conf.get('spark.sql.files.maxPartitionBytes')
display(bytesPartitions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable','PEOPLE2').option('user',user).option('password',password).option('driver',driver).load()
jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable','(select * from PEOPLE2)').option('user',user).option('password',password).option('driver',driver).load()
numPartitions = jdbcDF.rdd.getNumPartitions()
display(numPartitions)
#shufflePartitions = spark.conf.get('spark.sql.shuffle.partitions')
#display(shufflePartitions)
bytesPartitions = spark.conf.get('spark.sql.files.maxPartitionBytes')
display(bytesPartitions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable','PEOPLE3').option('user',user).option('password',password).option('partitionColumn','ID').option('numpartitions','80').option('lowerBound', '0').option('UpperBound', '20000000').option('driver',driver).load()
#jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable','PEOPLE2').option('user',user).option('password',password).option('partitionColumn','ID').option('numpartitions','40').option('driver',driver).load()
#jdbcDF = spark.read.format('jdbc').option('url',url).option('query',sql).option('user',user).option('password',password).option('driver',driver).load()
#display(jdbcDF.limit(10))  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
import io
import logging
import os

from operator import add
from pyspark.sql import SparkSession, Row
from pyspark.context import SparkContext
from datetime import datetime, timedelta
from pyspark.sql.functions import * 

import pandas as pd

from azure.identity import ClientSecretCredential
from azure.storage.blob import ContainerClient, BlobClient


host = '10.250.24.2'#ex033-scan-des-orc.corp.iberdrola.com' #
port = 1530  
service_name = 'cchd_bi.corp.iberdrola.com'
table = 'C0_LEC_HORARIA_VAL'


user = 'C0_CCH'  
password = 'C0_CCH'

start_date = datetime(2022, 1,4)
end_date = datetime(2022, 1, 4)
total_days = (end_date - start_date).days + 1


jdbcUrl = 'jdbc:oracle:thin:@//10.250.24.2:1530/cchd_bi.corp.iberdrola.com' 
jdbcDriver = 'oracle.jdbc.OracleDriver'

for deltaday in range (total_days): 
    current_date = (start_date + timedelta(days=deltaday)).date().isoformat()
    
    start_date = datetime(2022, 1, 4)
    end_date = datetime(2022, 1, 5)
    display(start_date)
    display(end_date)

    lowerday = "'20220104'"
    upperday = "'20220105'"
    model = "'YYYYMMDD'"
    display(lowerday)
    display(upperday)

    bounds_query = 'SELECT MIN(FEC_REGISTRO), MAX(FEC_REGISTRO) FROM C0_CCH.C0_LEC_HORARIA_VAL'
    jdbcDF_bounds = spark.read.format('jdbc').option('url',jdbcUrl).option('query',bounds_query).option('user',user).option('password',password).option('driver',jdbcDriver).load()
    display(jdbcDF_bounds)
    lowerBound = str(jdbcDF_bounds.collect()[0][0])
    upperBound = str(jdbcDF_bounds.collect()[0][1])
    display(lowerBound)
    display(upperBound)

    
    sql= f'(SELECT COD_PM,NUM_LINEA,COD_PAIS,COD_PROVINCIA,COD_POBLACION,NUM_CT,COD_METERID,FEC_LECTURA,MAG_LECTURA,FLG_INV_VER,FEC_REGISTRO,EST_LECTURA,VAL_R1,VAL_R2,VAL_R3,VAL_R4,FLG_ANULA,FEC_LECTURA_NOR,FEC_LEC_DIA_NOR,FEC_LEC_MES_NOR,NOM_FICHERO_STG FROM C0_CCH.C0_LEC_HORARIA_VAL WHERE FEC_REGISTRO >= to_date({lowerday},{model}) and FEC_REGISTRO < to_date({upperday},{model}))'
    display(sql)    
    jdbcDF = spark.read.format('jdbc').option('url',jdbcUrl).option('dbtable',sql).option("oracle.jdbc.mapDateToTimestamp", "true").option("sessionInitStatement", "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'").option('partitionColumn','FEC_REGISTRO').option('lowerBound', lowerBound).option('upperBound',upperBound).option('numPartitions',"200").option('user',user).option('password',password).option('driver',jdbcDriver).load()
    display(jdbcDF.limit(10))

 
    jdbcDF.write.partitionBy('FEC_REGISTRO').mode('overwrite').parquet('abfss://sinkspark@dlssypocnetworkingint.dfs.core.windows.net/psfrspark/')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
