# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fae45186-43c8-403a-811e-94ff186176da",
# META       "default_lakehouse_name": "volkswagen_lakehouse",
# META       "default_lakehouse_workspace_id": "17f9540b-2901-4593-9d80-f65a9ac92f93",
# META       "known_lakehouses": [
# META         {
# META           "id": "fae45186-43c8-403a-811e-94ff186176da"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "11332024-11c9-96df-49e4-f27722a580ef",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.version

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Spark version:", spark.version)

print("\nRelevant Spark configuration:")
for key, value in sorted(spark.sparkContext.getConf().getAll()):
    if (
        "spark.sql" in key
        or "delta" in key.lower()
        or "native" in key.lower()
    ):
        print(f"{key} = {value}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "dbo"

spark.sql(f"DESCRIBE DETAIL {table_name}").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Create a reasonably sized practice dataset
df = (
    spark.range(0, 100000)
    .withColumn("customer_id", (F.col("id") % 10000).cast("int"))
    .withColumn(
        "product_category",
        F.when(F.col("id") % 5 == 0, "Electronics")
         .when(F.col("id") % 5 == 1, "Clothing")
         .when(F.col("id") % 5 == 2, "Furniture")
         .when(F.col("id") % 5 == 3, "Books")
         .otherwise("Sports")
    )
    .withColumn("region",
        F.when(F.col("id") % 4 == 0, "North")
         .when(F.col("id") % 4 == 1, "South")
         .when(F.col("id") % 4 == 2, "East")
         .otherwise("West")
    )
    .withColumn("amount", (F.rand() * 5000).cast("double"))
    .withColumn(
        "order_date",
        F.expr("date_add('2024-01-01', CAST(id % 365 AS INT))")
    )
    .drop("id")
)

df.write.format("delta").mode("overwrite").saveAsTable("sales_optimization")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "<INSTRUCTOR_TABLE_NAME>"

spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "dbo"

spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

data = [
    Row(id=1, name="Alice", age=22, city="Mumbai"),
    Row(id=2, name="Bob", age=25, city="Delhi"),
    Row(id=3, name="Charlie", age=28, city="Pune"),
    Row(id=4, name="David", age=30, city="Nagpur"),
    Row(id=5, name="Emma", age=24, city="Bangalore")
]

df = spark.createDataFrame(data)

df.write.format("delta").mode("overwrite").saveAsTable("students")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "students"

spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history_df = spark.sql(f"""
DESCRIBE HISTORY {table_name}
""")

history_df.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history_df.select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Run:
detail_df = spark.sql(f"""
DESCRIBE DETAIL {table_name}
""")

detail_df.select(
    "format",
    "numFiles",
    "sizeInBytes",
    "partitionColumns",
    "location"
).show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

detail_df = spark.sql(f"""
DESCRIBE DETAIL {table_name}
""")

detail_df.select(
    "format",
    "numFiles",
    "sizeInBytes",
    "partitionColumns",
    "location"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table(table_name)

print("Number of Spark partitions:", df.rdd.getNumPartitions())

files = df.inputFiles()

print("Referenced files:", len(files))

for file_path in files[:20]:
    print(file_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

file_df = (
    spark.read.format("binaryFile")
    .load(files)
    .select("path", "length")
)

file_df.select(
    F.count("*").alias("file_count"),
    F.min("length").alias("min_bytes"),
    F.max("length").alias("max_bytes"),
    F.avg("length").alias("avg_bytes")
).show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_df.orderBy("length").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from time import perf_counter

start = perf_counter()

result = (
    spark.table("students")
    .filter("age >= 25")
    .groupBy("city")
    .count()
)

result.show(truncate=False)

elapsed = perf_counter() - start

print(f"Execution time: {elapsed:.2f} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result.explain("formatted")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
SHOW TBLPROPERTIES {table_name}
""").show(truncate=False)
Look for:
delta.parquet.vorder.enabled
Also inspect:
print(
    "Session V-Order:",
    spark.conf.get(
        "spark.sql.parquet.vorder.default",
        "not explicitly configured"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "students"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
SHOW TBLPROPERTIES {table_name}
""").show(truncate=False)

print(
    "Session V-Order:",
    spark.conf.get(
        "spark.sql.parquet.vorder.default",
        "not explicitly configured"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
ALTER TABLE {table_name}
SET TBLPROPERTIES (
    'delta.parquet.vorder.enabled' = 'true'
)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
SHOW TBLPROPERTIES {table_name}
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
OPTIMIZE {table_name}
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").select(
    "numFiles",
    "sizeInBytes"
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_after = spark.table(table_name).inputFiles()

print("Files after OPTIMIZE:", len(files_after))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table(table_name).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
OPTIMIZE {table_name}
ZORDER BY (<COLUMN_1>, <COLUMN_2>)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
OPTIMIZE {table_name}
ZORDER BY (age, city)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
SHOW TBLPROPERTIES {table_name}
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
ALTER TABLE {table_name}
CLUSTER BY (<COLUMN_1>, <COLUMN_2>)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
ALTER TABLE {table_name}
CLUSTER BY (age, city)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
SHOW TBLPROPERTIES {table_name}
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Incremental clustering:",
    spark.conf.get(
        "spark.microsoft.delta.optimize.clustering.strategy.incremental",
        "default/not explicitly configured"
    )
)

print(
    "Auto recluster:",
    spark.conf.get(
        "spark.microsoft.delta.optimize.clustering.strategy.incremental.autoRecluster",
        "default/not explicitly configured"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    print(
        "Incremental clustering:",
        spark.conf.get(
            "spark.microsoft.delta.optimize.clustering.strategy.incremental"
        )
    )
except:
    print("Incremental clustering: not explicitly configured")

try:
    print(
        "Auto recluster:",
        spark.conf.get(
            "spark.microsoft.delta.optimize.clustering.strategy.incremental.autoRecluster"
        )
    )
except:
    print("Auto recluster: not explicitly configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
OPTIMIZE {table_name}
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
ALTER TABLE {table_name}
CLUSTER BY (city, age)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
OPTIMIZE {table_name} FULL
""")
Then inspect:
spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history_df = spark.sql(f"""
DESCRIBE HISTORY {table_name}
""")

history_df.select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
VACUUM {table_name} RETAIN 168 HOURS
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "<TARGET_TABLE>"

print(f"Starting maintenance for {table_name}")

spark.sql(f"""
OPTIMIZE {table_name}
""")

print("OPTIMIZE completed")

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "students"

print(f"Starting maintenance for {table_name}")

spark.sql(f"""
OPTIMIZE {table_name}
""")

print("OPTIMIZE completed")

spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "students"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "dbo.students"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "dbo.students"

spark.sql(f"SELECT * FROM {table_name} LIMIT 5").show()

spark.sql(f"""
OPTIMIZE {table_name}
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tablename = "students"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES IN dbo").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM dbo.students")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tablename = "students"

df = spark.sql(f"SELECT * FROM dbo.{tablename}")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
