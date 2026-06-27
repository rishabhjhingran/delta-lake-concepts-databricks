# Databricks notebook source
# MAGIC %run '/Workspace/Users/rishabh.jhingran@outlook.com/Delta Lake Concepts/ACID Properties/ACID Properties'

# COMMAND ----------

# MAGIC %run "/Workspace/Users/rishabh.jhingran@outlook.com/Delta Lake Concepts/ACID Properties/ACID Properties"

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/Volumes/workspace/default/acid_demo/orders_managed`
# MAGIC SET product_qty=product_qty+10;
# MAGIC INSERT INTO delta.`/Volumes/workspace/default/acid_demo/orders_managed`
# MAGIC VALUES(7,'sku-7','New Item','Misc',2,500.00)