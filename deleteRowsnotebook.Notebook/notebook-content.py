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

# ## Notebook to clean the table before  to Oracle 19 under **VPN** with no public access 


# MARKDOWN ********************

# Download JDBC driver from --> https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

# PARAMETERS CELL ********************

partitionnumber = "73202" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import col

deltaTable = DeltaTable.forPath(spark, 'abfss://Oracletestundervpn@onelake.dfs.fabric.microsoft.com/landingOraData.Lakehouse/Tables/sales1')

# Delete rows based on a condition
deltaTable.delete(col('partitionnumber') = "73202")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable.delete(col('partitionnumber') == "73202")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
