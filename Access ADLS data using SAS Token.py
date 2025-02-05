# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS Token
# MAGIC
# MAGIC Set the spark config for SAS Token
# MAGIC
# MAGIC List files from demo container - sap-test
# MAGIC
# MAGIC Read data from circuits.csv file

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.etlfwstudiodevadls.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.etlfwstudiodevadls.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.etlfwstudiodevadls.dfs.core.windows.net", "sp=rl&st=2024-11-20T08:55:53Z&se=2024-11-20T16:55:53Z&spr=https&sv=2022-11-02&sr=c&sig=aKURdHnnS8Y1y7ZndRxCqPtnkmFu8dI80kTRzAIsPi4%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/circuits.csv"))
