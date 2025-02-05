-- Databricks notebook source
show databases

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use demo

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

select * from circuits_sql limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AND Command

-- COMMAND ----------

select * from circuits_sql where country  = 'Australia' and location = 'Melbourne'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OR Command

-- COMMAND ----------

select * from circuits_sql where country  = 'Australia' or location = 'Istanbul'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Combination of AND OR

-- COMMAND ----------

select * from circuits_sql where (country  = 'Australia' and circuitId = 1) or location = 'Istanbul'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Order by  Bydefault - Ascending

-- COMMAND ----------

select * from circuits_sql order by country asc, location desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Concatenation**

-- COMMAND ----------

select circuitId, concat(lat, ',',lng) as Location from circuits_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Split**

-- COMMAND ----------

SELECT circuitId, split(name, ' ') FROM circuits_sql

-- COMMAND ----------

SELECT *, split(name, ' ')[0] forename, split(name, ' ')[1] forename FROM circuits_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Current Timestamp, Current Timezone

-- COMMAND ----------

SELECT *, current_timestamp(), current_timezone() AS current_timezone FROM circuits_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ## change dateformats

-- COMMAND ----------

SELECT *,current_timestamp(),  substr(current_timestamp(),0,10) FROM circuits_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Split Timestamp

-- COMMAND ----------

SELECT *,current_timestamp(),  date_format(substr(split(current_timestamp(),"T")[0],0,10), 'dd-MM-yyyy') as Date  FROM circuits_sql

-- COMMAND ----------

SELECT *,current_timestamp(),  split(current_timestamp(), 'T')[0] currentDate FROM circuits_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aggregations

-- COMMAND ----------

SELECT country,max(alt) FROM circuits_sql GROUP BY country

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **window Functions**

-- COMMAND ----------

SELECT country,name,rank(alt) OVER (PARTITION BY country ORDER BY alt DESC)  FROM circuits_sql
 ORDER BY country
