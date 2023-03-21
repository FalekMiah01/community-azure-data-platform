# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Delta Cache and Spark Cache
# MAGIC 
# MAGIC This notebook is to demonstrate **`Spark Cache`** for **Databricks Delta Cache and Spark Cache** comparison. 
# MAGIC 
# MAGIC It will use **`NYCTaxi-TripData`** as source dataset from **databricks-dataset:** `'/databricks-datasets/nyctaxi/tripdata/fhv/fhv_tripdata_2019-12.csv.gz'` <br>

# COMMAND ----------

# %fs ls /databricks-datasets/nyctaxi/tripdata/fhv/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load Data into Dataframe

# COMMAND ----------

sourcePath    = '/databricks-datasets/nyctaxi/tripdata/fhv/fhv_tripdata_2019-12.csv.gz'
df_taxiSource = spark.read.format('csv').options(header='true', inferSchema='true').load(sourcePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Delta

# COMMAND ----------

deltaPath    = "/mnt/fmsandbox/nyctaxi/tripdata/"
databaseName = "fmsandbox"
tableName    = "tripdata"

#################################################
## Create Database and Set to use
#################################################
spark.sql(f"""
  CREATE DATABASE IF NOT EXISTS {databaseName}
  """)

spark.sql(f"USE {databaseName};")

#################################################
## Check if table exists in database else create
#################################################
databasesList = spark.sql(f"""show tables in {databaseName}""")
tableList = databasesList.filter(databasesList.tableName == tableName).collect()

if len(tableList) > 0:
    print("Delta table exists")
else:
    ## Create Delta File
    ( df_taxiSource.write
       .format("delta")
       .mode("overwrite") 
       .save(deltaPath) )

    ## Create Delta Table
    spark.sql("""
        CREATE TABLE {0}.{1} 
        USING DELTA 
        LOCATION '{2}'
        """.format(databaseName, tableName, deltaPath))
    print("Delta table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Cache

# COMMAND ----------

spark.sql(f"USE fmsandbox;")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pre Cache

# COMMAND ----------

preCache = spark.sql("SELECT count(*) FROM tripdata")
display(preCache)

# COMMAND ----------

# MAGIC %md
# MAGIC View the Cluster Spark UI > Storage

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Post Cache

# COMMAND ----------

postCache = spark.sql("SELECT count(*) FROM tripdata")

# COMMAND ----------

postCache = postCache.cache()
display(postCache)

# COMMAND ----------

# MAGIC %md
# MAGIC View the Cluster Spark UI > Storage

# COMMAND ----------

postCache.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC View the Cluster Spark UI > Storage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up
# MAGIC 
# MAGIC Unpersist cached dataframes

# COMMAND ----------

postCache.unpersist()

# COMMAND ----------

# # Remove delta table and files
# spark.sql("DROP TABLE IF EXISTS " + "fmsandbox.tripdata")
# dbutils.fs.rm("/mnt/fmsandbox/nyctaxi/tripdata/", True)
