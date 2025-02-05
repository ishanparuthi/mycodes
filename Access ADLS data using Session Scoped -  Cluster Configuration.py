# Databricks notebook source
display(dbutils.fs.ls("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://sap-test@etlfwstudiodevadls.dfs.core.windows.net/circuits.csv"))
