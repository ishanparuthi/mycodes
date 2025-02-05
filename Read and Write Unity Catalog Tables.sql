-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### One way -  Provide full Names along with Catalog and Schema

-- COMMAND ----------

Select * from catalog_test01.demo.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### another way- set current catalog and current Schema

-- COMMAND ----------

Use Catalog catalog_test01;
Use Schema demo;

-- COMMAND ----------

SELECT * FROM circuits

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

SELECT current_schema()

-- COMMAND ----------

show catalogs

-- COMMAND ----------

show schemas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reading data from Managed Table using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql('Show Tables'))   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("catalog_test01.demo.circuits")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Catalog

-- COMMAND ----------

show catalogs

-- COMMAND ----------

Use Catalog `sap-test`

-- COMMAND ----------

Create schema if not exists ip_schema
managed location "abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Tables

-- COMMAND ----------

drop table if exists `sap-test`.ip_schema.drivers;
Create table if not exists `sap-test`.ip_schema.drivers
( 
  driverId int,
  driverRef string,
  number int,
  code string,
  name Struct<forname: String, surname: String>,
  dob date,
  nationality string,
  url string
)
using json
options(path "abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/raw/drivers.json")

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Managed Tables

-- COMMAND ----------

drop table if exists `sap-test`.ip_schema.drivers_managed;
create table if not exists `sap-test`.ip_schema.drivers_managed
AS 
Select driverId as driver_id,
driverRef as driver_ref,
number,
code,
concat(name.forname, ' ', name.surname) as name,
dob,
nationality,
current_timestamp as ingestion_date
 from `sap-test`.ip_schema.drivers

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers_managed

-- COMMAND ----------

select * from system.information_schema.tables where table_name  = 'drivers'

-- COMMAND ----------

select * from `sap-test`.information_schema.tables where table_name  = 'drivers'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reading and Writing to Delta Lake

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.option("inferschema", True).json("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Creating Temp View**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.createOrReplaceTempView('temp_view')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create managed table from Temp View

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # In Python
-- MAGIC results_df.write.format("delta").mode("overwrite").saveAsTable("`sap-test`.ip_schema.drivers_view_sql")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### In SQL

-- COMMAND ----------

-- IN SQL
Create table `sap-test`.ip_schema.drivers_view as select * from temp_view

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Table using Partition

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").partitionBy("nationality").saveAsTable("`sap-test`.ip_schema.drivers_view_partition")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data is saved acc to Paritition

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers_view_partition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update Managed Table

-- COMMAND ----------

update `sap-test`.ip_schema.drivers_view
set url = 'Changed Entry' 
Where driverId = 1

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC deltaTable = DeltaTable.forPath(spark, "`sap-test`.ip_schema/drivers_view")
-- MAGIC # deltaTable = DeltaTable.forPath(spark, "abfss://sap-test@your-storage-account.dfs.core.windows.net/ip_schema/drivers_view_partition")
-- MAGIC deltaTable.update("driverId  =2", {"url" : "updated using Python"})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Delete From Managed Table

-- COMMAND ----------

delete from `sap-test`.ip_schema.drivers_view
Where driverId = 2;

-- COMMAND ----------

Select * from `sap-test`.ip_schema.drivers_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Upsert Statements

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_set1 = spark.read.option("inferschema", True).json("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/raw/drivers.json").filter("driverId <= 10")
-- MAGIC display(df_set1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_set2 = spark.read.option("inferschema", True).json("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/raw/drivers.json").filter("driverId > 8")
-- MAGIC display(df_set2)

-- COMMAND ----------

create table `sap-test`.ip_schema.drivers_merge as select * from `sap-test`.ip_schema.drivers_view

-- COMMAND ----------

truncate table `sap-test`.ip_schema.drivers_merge

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_set1.createOrReplaceTempView("df_set1View")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_set2.createOrReplaceTempView("df_set2View")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insert in Upsert

-- COMMAND ----------

MERGE into `sap-test`.ip_schema.drivers_merge tgt
using df_set1View upd
on tgt.driverId = upd.driverId
when matched then 
update set tgt.code = upd.code
When not matched then
Insert *;

-- COMMAND ----------

SELECT * from `sap-test`.ip_schema.drivers_merge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update in Upsert

-- COMMAND ----------

MERGE into `sap-test`.ip_schema.drivers_merge tgt
using df_set2View upd
on tgt.driverId = upd.driverId
when matched then 
update set tgt.code = upd.code
When not matched then
Insert *;

-- COMMAND ----------

SELECT * from `sap-test`.ip_schema.drivers_merge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checking History of Table

-- COMMAND ----------

describe history `sap-test`.ip_schema.drivers_merge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time Travel

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers_merge version as of 3

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers_merge timestamp as of '2025-01-06T12:32:55.000+00:00'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vacuum Command

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vacuum Removes data by default older than 7 Days

-- COMMAND ----------

vacuum `sap-test`.ip_schema.drivers_merge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **To Delete all the history, use Retain with Vacuum**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
-- MAGIC spark.sql("VACUUM `sap-test`.ip_schema.drivers_merge RETAIN 0 HOURS")
-- MAGIC

-- COMMAND ----------

describe history `sap-test`.ip_schema.drivers_merge

-- COMMAND ----------

select * from `sap-test`.ip_schema.drivers_merge version as of 3

-- COMMAND ----------

CREATE table `sap-test`.ip_schema.parquettodelta (
  DriverId int,
  dob Date,
  forname String,
  surname STRING,
)
Using Parquet

-- COMMAND ----------

show create table `sap-test`.ip_schema.drivers_merge

-- COMMAND ----------

CREATE TABLE `sap-test`.ip_schema.drivers_parquertable (
  code STRING,
  dob STRING,
  driverId BIGINT,
  driverRef STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  nationality STRING,
  number STRING,
  url STRING)
USING PARQUET


-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop Duplicates

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_noduplicates = df.dropDuplicates(['country'])
-- MAGIC display(df_noduplicates)

-- COMMAND ----------

SELECT * FROM `sap-test`.ip_schema.drivers


-- COMMAND ----------

SELECT nationality, count(1) FROM `sap-test`.ip_schema.drivers GROUP BY nationality


-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.option("inferschema", True).parquet("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/SQLData/*.parquet")
-- MAGIC display(results_df)

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Type of Views- 
-- MAGIC 1. Local Temp View
-- MAGIC 2. Global Temp View

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.option("inferschema", True).parquet("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/SQLData/*.parquet")
-- MAGIC display(results_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Local Temp View

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.createOrReplaceTempView("results_df_view")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reading View using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df  = spark.sql("select * from results_df_view")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reading View using SQL

-- COMMAND ----------

select * from results_df_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Global Temp View

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.createOrReplaceGlobalTempView("results_df_global")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("SHOW TABLES").show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("SHOW TABLES IN global_temp").show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df =  spark.sql("select * from global_temp.results_df_global")
-- MAGIC display(df)
