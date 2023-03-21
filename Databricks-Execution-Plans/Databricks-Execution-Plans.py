# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Execution Plans
# MAGIC 
# MAGIC Execution Plans allow you to understand how code is actually executed across a cluster.  
# MAGIC 
# MAGIC This notebook is to demonstrate **`Databricks Execution Plans`** including the following:
# MAGIC - Generating Execution Plans using `.explain()` for Physical and Logical Plans
# MAGIC - Additional Parameters for `.explain()`
# MAGIC 
# MAGIC Fake data will be generated and store as Delta tables. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Sample Data and Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Generate Sample Data and Delta tables 
# MAGIC 
# MAGIC Create `sales` and `items` table with fake data. 

# COMMAND ----------

# Drop "items" and "sales" delta table
spark.sql("""DROP TABLE IF EXISTS items;""")
spark.sql("""DROP TABLE IF EXISTS sales;""")

# Create `items` delta table
spark.sql("""
  CREATE TABLE items
  USING delta
  AS
  SELECT   CAST( id + 1 AS integer) AS itemID
           , CAST( CONCAT('ITEM-', id + 1) AS string) AS itemName
           , CAST(rand() * 1000 AS float) AS price
           , current_date() AS effectiveDate
  FROM RANGE(5);
""")
print(f"Created `items` delta table")

# Create `sales` delta table with skew data - Item with id 100 is in 80% of all sales.
spark.sql("""
CREATE TABLE sales
USING delta
AS
SELECT 
  CASE WHEN rand() < 0.5 THEN 4 ELSE CAST(rand() * 10 AS INT) END AS itemID,
  CAST(rand() * 25 AS INT) AS quantity,
  DATE_ADD(current_date(), - CAST(rand() * 360 AS INT)) AS saleDate
FROM RANGE(50);
""")
print(f"Created `sales` delta table")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a Dataframe from Delta tables
# MAGIC 
# MAGIC Join `sales` table with `items` table, sum the sales `quantity` by items `price` and group by `salesDate`

# COMMAND ----------

df = spark.sql("""SELECT saleDate
                       , sum(quantity * price) AS totalSales
                  FROM sales s
                  JOIN items i ON i.itemID = s.itemID
                  WHERE i.itemID = 4
                  GROUP BY saleDate
                  ORDER BY totalSales DESC;
                  """)
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Execution Plans
# MAGIC 
# MAGIC Return Logical and/or Physical plans use the `.explain()` function.  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explore the Physical Plan using `.explain()`
# MAGIC 
# MAGIC If no argument is provided then `.explain()` function will return the Physical Plan by default.  Showing the Sort, Aggregate, Filter and FileScan the query executed.  

# COMMAND ----------

df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explore the Logical Plan using `.explain(True)`
# MAGIC 
# MAGIC By providing a `True` argument in `.explain()` function, it will return the Logical Plan.  Showing the `Parsed Logical Plan`, `Analyzed Logical Plan`, `Optimized Logical Plan` and `Physical Plan`. 

# COMMAND ----------

df.explain(True)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Additional Parameters
# MAGIC 
# MAGIC The `mode` argument can be applied to `.explain()` function which will show different output format of plans.  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explore the Logical Plan using `.explain("extended")`
# MAGIC 
# MAGIC Displays both logical and physical plans, like providing `True` arguments. 

# COMMAND ----------

df.explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Codegen mode using `.explain("codegen")`
# MAGIC 
# MAGIC Displays the physical plan and generated codes if they are available.

# COMMAND ----------

df.explain(mode="codegen")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cost mode using `.explain("cost")`
# MAGIC 
# MAGIC Displays the optimized logical plan and related statistics, if they exist. 

# COMMAND ----------

df.explain(mode="cost")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Formatted mode using `.explain("formatted")`
# MAGIC 
# MAGIC Displays two sections splitting Physical Plan outline and node details

# COMMAND ----------

df.explain(mode="formatted")
