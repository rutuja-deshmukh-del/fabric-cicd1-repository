# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bdd109d2-36a8-4e64-a654-fd508323c705",
# META       "default_lakehouse_name": "RetailLakehouse",
# META       "default_lakehouse_workspace_id": "17f9540b-2901-4593-9d80-f65a9ac92f93",
# META       "known_lakehouses": [
# META         {
# META           "id": "bdd109d2-36a8-4e64-a654-fd508323c705"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession

sales_data = [
    (1, "Laptop", 50000),
    (2, "Phone", 30000),
    (3, "Tablet", 20000),
    (4, "Monitor", 15000),
    (5, "Keyboard", 5000)
]

columns = ["id", "product", "sales"]

sales_df = spark.createDataFrame(sales_data, columns)

sales_df.show()

sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_sales")




sales_df.write.mode("overwrite").saveAsTable("retail_sales")

spark.sql("SHOW TABLES").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
