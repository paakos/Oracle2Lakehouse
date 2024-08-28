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

# ## Notebook to connect to get the table partitions to do a for each with the result executing paralell download with a Fabric Pipeline 


# MARKDOWN ********************

# Download JDBC driver from --> https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

# CELL ********************

from pyspark.sql import SparkSession
import pandas as pd

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
driver = 'oracle.jdbc.driver.OracleDriver'
sql = "(SELECT CAST(DBMS_ROWID.ROWID_OBJECT(ROWID) AS INTEGER) AS partition_id FROM Sales1 GROUP BY DBMS_ROWID.ROWID_OBJECT(ROWID) ORDER  BY 1 ASC)"
#sql='SELECT DBMS_ROWID.ROWID_OBJECT(ROWID) AS partition_id FROM Sales1 GROUP BY DBMS_ROWID.ROWID_OBJECT(ROWID);' # with dbtable

jdbcDF = spark.read.format('jdbc').option('url',url).option('dbtable',sql).option('user',user).option('password',password).option('driver',driver).load()
#jdbcDF1 = spark.read.format('jdbc').option('url',url).option('dbtable',sql).option('user',user).option('password',password).option('partitionColumn','ONLINESALESKEY').option('numpartitions','10').option("lowerBound", '0').option("UpperBound", "32188091").option('driver',driver).load()
#display(jdbcDF)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from decimal import Decimal
data_list = jdbcDF.toPandas().to_dict(orient='records')
#display(data_list)
partition_ids = [item['PARTITION_ID'] for item in data_list]
partition_ids = [int(item['PARTITION_ID']) for item in data_list]
#display(partition_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#import json

#json_str = jdbcDF.toJSON().collect()
#json_output = json.dumps(json_str)
#display(json_output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#mssparkutils.notebook.exit(str(temptablename))
#mssparkutils.notebook.exit(str(json_output))
mssparkutils.notebook.exit(partition_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
