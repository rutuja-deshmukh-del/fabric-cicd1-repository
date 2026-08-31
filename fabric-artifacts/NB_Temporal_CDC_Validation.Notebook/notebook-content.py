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
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# Controlled CDC-style source changes for this validation lab
events = [
    (101, "Alice", "Pune",   "INSERT", "2026-08-31T09:00:00", 1),
    (101, "Alice", "Mumbai", "UPDATE", "2026-08-31T09:05:00", 2),
    (102, "Bob",   "Delhi",  "INSERT", "2026-08-31T09:02:00", 3)
]

schema = """
BusinessKey INT,
CustomerName STRING,
Region STRING,
Operation STRING,
ChangeTimestamp STRING,
SequenceNo INT
"""

bronze = spark.createDataFrame(events, schema)

bronze = (
    bronze
    .withColumn("ChangeTimestamp", F.to_timestamp("ChangeTimestamp"))
    .withColumn("IngestionTimestamp", F.current_timestamp())
)

# Create Bronze CDC table
bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Bronze_CDC")

# Create Silver Temporal table
spark.sql("DROP TABLE IF EXISTS Silver_Temporal")

spark.sql("""
CREATE TABLE Silver_Temporal (
    BusinessKey INT,
    CustomerName STRING,
    Region STRING,
    EffectiveFrom TIMESTAMP,
    EffectiveTo TIMESTAMP,
    IsCurrent BOOLEAN
)
USING DELTA
""")

print("Bronze_CDC and Silver_Temporal created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read the Bronze CDC table
bronze_df = spark.table("Bronze_CDC")

# Select only the initial INSERT records
initial = (
    bronze_df
    .filter("Operation = 'INSERT'")
    .withColumn("EffectiveFrom", F.col("ChangeTimestamp"))
    .withColumn(
        "EffectiveTo",
        F.to_timestamp(F.lit("9999-12-31 23:59:59"))
    )
    .withColumn("IsCurrent", F.lit(True))
    .select(
        "BusinessKey",
        "CustomerName",
        "Region",
        "EffectiveFrom",
        "EffectiveTo",
        "IsCurrent"
    )
)

# Write the initial versions into the Silver table
initial.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("Silver_Temporal")

print("Initial SCD Type 2 state created successfully.")

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

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Load the Silver Delta table
target = DeltaTable.forName(spark, "Silver_Temporal")

# Get the UPDATE event for BusinessKey 101
update_row = (
    spark.table("Bronze_CDC")
    .filter("BusinessKey = 101 AND Operation = 'UPDATE'")
)

# Close the existing current version
(
    target.alias("t")
    .merge(
        update_row.alias("s"),
        "t.BusinessKey = s.BusinessKey AND t.IsCurrent = true"
    )
    .whenMatchedUpdate(
        set={
            "EffectiveTo": "s.ChangeTimestamp",
            "IsCurrent": "false"
        }
    )
    .execute()
)

# Create the new current version
new_version = (
    update_row
    .select(
        "BusinessKey",
        "CustomerName",
        "Region",
        "ChangeTimestamp"
    )
    .withColumnRenamed("ChangeTimestamp", "EffectiveFrom")
    .withColumn(
        "EffectiveTo",
        F.to_timestamp(F.lit("9999-12-31 23:59:59"))
    )
    .withColumn("IsCurrent", F.lit(True))
)

# Add the new version to Silver
new_version.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("Silver_Temporal")

print("SCD Type 2 update applied successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("Silver_Temporal"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("Silver_Temporal")
    .orderBy("BusinessKey", "EffectiveFrom")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver = "Silver_Temporal"

history = spark.sql(f"DESCRIBE HISTORY {silver}")

history.select(
    "version",
    "timestamp",
    "operation"
).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VERSION_A = 1

old_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", VERSION_A)
    .table("Silver_Temporal")
)

old_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

silver = "Silver_Temporal"

# Get latest Delta version
history = spark.sql(f"DESCRIBE HISTORY {silver}")

latest = (
    history
    .orderBy("version", ascending=False)
    .select("version", "timestamp", "operation")
    .first()
)

print("Latest Delta Version:", latest["version"])
print("Latest Version Timestamp:", latest["timestamp"])
print("Latest Operation:", latest["operation"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver = "Silver_Temporal"

history = spark.sql(f"DESCRIBE HISTORY {silver}")

history.select(
    "version",
    "timestamp",
    "operation"
).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VERSION_A = 1

print("Earlier Delta Version:", VERSION_A)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

old_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", VERSION_A)
    .table("Silver_Temporal")
)

old_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TIMESTAMP_A = "2026-08-31 11:06:31.412"

timestamp_df = (
    spark.read
    .format("delta")
    .option("timestampAsOf", TIMESTAMP_A)
    .table("Silver_Temporal")
)

timestamp_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

detail = spark.sql("DESCRIBE DETAIL Silver_Temporal").collect()[0]

print("Location:", detail["location"])
print("Format:", detail["format"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

detail = spark.sql("DESCRIBE DETAIL Silver_Temporal").collect()[0]
location = detail["location"]

log_path = location + "/_delta_log"

print("Delta log location:")
print(log_path)

print("\nFiles in _delta_log:")
for f in mssparkutils.fs.ls(log_path):
    print(f.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = mssparkutils.fs.ls(log_path)

json_versions = []
checkpoint_versions = []

for f in files:
    name = f.name

    if name.endswith(".json") and name[:20].isdigit():
        json_versions.append(int(name[:20]))

    if ".checkpoint." in name and name[:20].isdigit():
        checkpoint_versions.append(int(name[:20]))

print("Latest Delta version:", max(json_versions) if json_versions else "None")

if checkpoint_versions:
    print("Latest checkpoint version:", max(checkpoint_versions))
else:
    print("Latest checkpoint: None")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

point_in_time = "2026-08-31 09:02:00"

display(
    spark.table("Silver_Temporal")
    .filter(
        (F.col("BusinessKey") == 101) &
        (F.col("EffectiveFrom") <= F.to_timestamp(F.lit(point_in_time))) &
        (F.col("EffectiveTo") > F.to_timestamp(F.lit(point_in_time)))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("Silver_Temporal")
    .orderBy("BusinessKey", "EffectiveFrom")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window

w = Window.partitionBy("BusinessKey").orderBy("EffectiveFrom")

gap_check = (
    spark.table("Silver_Temporal")
    .withColumn("NextEffectiveFrom", F.lead("EffectiveFrom").over(w))
    .filter(
        F.col("NextEffectiveFrom").isNotNull() &
        (F.col("EffectiveTo") != F.col("NextEffectiveFrom"))
    )
    .select(
        "BusinessKey",
        "EffectiveFrom",
        "EffectiveTo",
        "NextEffectiveFrom"
    )
)

display(gap_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("BusinessKey").orderBy("EffectiveFrom")

overlap_check = (
    spark.table("Silver_Temporal")
    .withColumn("PreviousEffectiveTo", F.lag("EffectiveTo").over(w))
    .filter(
        F.col("PreviousEffectiveTo").isNotNull() &
        (F.col("EffectiveFrom") < F.col("PreviousEffectiveTo"))
    )
    .select(
        "BusinessKey",
        "EffectiveFrom",
        "EffectiveTo",
        "PreviousEffectiveTo"
    )
)

display(overlap_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver = "Silver_Temporal"

# Get the current Delta history
history = spark.sql(f"DESCRIBE HISTORY {silver}")

display(
    history.select("version", "timestamp", "operation")
           .orderBy("version")
)

# Automatically choose the earliest available version
earliest_version = (
    history.select(F.min("version").alias("min_version"))
           .collect()[0]["min_version"]
)

print("Earliest available Delta version:", earliest_version)

# Read that real available version
old_df = (
    spark.read
         .format("delta")
         .option("versionAsOf", earliest_version)
         .table(silver)
)

display(old_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VERSION_A = 1

old_df = (
    spark.read
         .format("delta")
         .option("versionAsOf", VERSION_A)
         .table("Silver_Temporal")
)

display(old_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history = spark.sql("DESCRIBE HISTORY Silver_Temporal")

history.select("version", "timestamp", "operation") \
       .orderBy("version") \
       .show(20, truncate=False)
       

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

timestamp_a = "2026-08-31 11:47:25"

timestamp_df = (
    spark.read
         .format("delta")
         .option("timestampAsOf", timestamp_a)
         .table("Silver_Temporal")
)

display(timestamp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
