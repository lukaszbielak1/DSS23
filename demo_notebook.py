# Databricks notebook source
# MAGIC %run ./connection 

# COMMAND ----------

spark.readStream\
.format("cloudFiles")\
.option("cloudFiles.format", "json")\
.schema(json_schema)\
.load(adls_path+"/*")\
.writeStream\
.format("delta")\
.outputMode("append")\
.option("checkpointLocation", "/tmp/delta/_checkpoints/sales_lt")\
.toTable("saleslt_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, to_timestamp(ts_ms/1000)from saleslt_customer order by ts_ms desc
