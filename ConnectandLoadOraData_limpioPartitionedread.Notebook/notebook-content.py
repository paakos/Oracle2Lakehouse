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

# 
# #### Run the cell below to install the required packages for Copilot


# MARKDOWN ********************

# ## Notebook to conect to Oracle 19 under **VPN** with no public access 


# MARKDOWN ********************

# Download JDBC driver from --> https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

# PARAMETERS CELL ********************

partitionnumber = 73202 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Testing conectivity to ip
# ### Revisar el parametro numero de particiones para acomodarlo al numero de nodos del cluster


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

#executing on Iberdrola env 
# url = "jdbc:oracle:thin:@//host:1521/db"
url= 'jdbc:oracle:thin:@4.223.225.126:1521/oratest1' 
user ='system'
password ='OraPasswd1'
#driver = 'oracle.jdbc.driver.0racleDriver'
driver = 'oracle.jdbc.driver.OracleDriver'

sql=f'(select * from Sales1 WHERE DBMS_ROWID.ROWID_OBJECT(ROWID) ={partitionnumber} )' # with dbtable

#jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable',sql).option('user',user).option('password',password).option('driver',driver).load()
jdbcDF1 = spark.read.format('jdbc').option('url',url).option('dbtable',sql).option('user',user).option('password',password).option('partitionColumn','ONLINESALESKEY').option('numpartitions','10').option("lowerBound", '0').option("UpperBound", "32188091").option('driver',driver).load()
numPartitions = jdbcDF1.rdd.getNumPartitions()
bytesPartitions = spark.conf.get('spark.sql.files.maxPartitionBytes')
#display(bytesPartitions)
print(f"Number of Partition -> {numPartitions} BytesperPartition {bytesPartitions} ")
display(jdbcDF1.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbcDF1.write.format("delta").mode("append").partitionBy("ORDERDATE").saveAsTable("Sales1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
