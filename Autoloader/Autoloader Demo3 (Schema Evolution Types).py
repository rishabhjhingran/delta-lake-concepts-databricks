# Databricks notebook source
# MAGIC %md
# MAGIC ### AUTOLOADER : SCHEMA EVOLUTION MODES
# MAGIC It has 4 schema evolution modes -
# MAGIC 1. none
# MAGIC 2. failOnNewColumns
# MAGIC 3. rescue
# MAGIC 4. addNewColumn(default that we have seen in all other deoms where we have to retry the readStream and it works)

# COMMAND ----------

# MAGIC %md
# MAGIC > [none]
# MAGIC - it doesnot evolve the schema.
# MAGIC - new columns are ignored and the data is not rescued until we explicitly set it.
# MAGIC - stream doesnot fails.
# MAGIC
# MAGIC > For this demo
# MAGIC - delete the volume files 3 and 4
# MAGIC - delete the checkpoint location
# MAGIC - drop the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_delta;

# COMMAND ----------

landingfilepath='/Volumes/workspace/default/autoloader'
orders=landingfilepath+'/orders'
checkpointlocation=landingfilepath+'/orders_checkpoint'


# COMMAND ----------

order_df=(spark.readStream
          .format('cloudFiles')
          .option('cloudFiles.format','csv')
          .option('cloudFiles.schemaLocation',checkpointlocation)
          .option('cloudFiles.inferSchema','true')
          .option('cloudFiles.inferColumnTypes','true')
          .option('cloudFiles.schemaEvolutionMode','none')
          .load(orders))

(order_df.writeStream
        .format('delta')
        .option('checkpointLocation',checkpointlocation)
        .option('mergeSchema','true')
        .outputMode('append')
        .trigger(availableNow=True)
        .toTable('orders_delta')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets add the third file to the volume and the difference 

# COMMAND ----------

(order_df.writeStream
        .format('delta')
        .option('checkpointLocation',checkpointlocation)
        .option('mergeSchema','true')
        .outputMode('append')
        .trigger(availableNow=True)
        .toTable('orders_delta')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC We could clearly see that there is no error in writeStream and if we view the data we find that it doesnot have the column and since we have not explicitly added any _rescued_column we dont see it .
# MAGIC
# MAGIC > Now lets add the fourth file and see the difference
# MAGIC
# MAGIC

# COMMAND ----------

(order_df.writeStream
        .format('delta')
        .option('checkpointLocation',checkpointlocation)
        .option('mergeSchema','true')
        .outputMode('append')
        .trigger(availableNow=True)
        .toTable('orders_delta')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see that the value that is corrupt is replaced with null.

# COMMAND ----------

# MAGIC %md
# MAGIC > **[failOnNewColumns]**
# MAGIC - stream fails
# MAGIC - streams doesnot restart unless the provided schema is updated or offending file is removed.
# MAGIC
# MAGIC > For this demo
# MAGIC - delete the volume files 3 and 4
# MAGIC - delete the checkpoint location
# MAGIC - drop the table
# MAGIC - we will just change the value schemaEvolutionMode to failOnNewColumns

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_delta;

# COMMAND ----------

order_df=(spark.readStream
          .format('cloudFiles')
          .option('cloudFiles.format','csv')
          .option('cloudFiles.schemaLocation',checkpointlocation)
          .option('cloudFiles.schemaEvolutionMode','failOnNewColumns')
          .option('cloudFiles.inferSchema','true')
          .option('cloudFiles.inferColumnTypes','true')
          .load(orders))

(order_df.writeStream
.format('delta')
.option('checkpointLocation',checkpointlocation)
.option('mergeSchema','true')
.trigger(availableNow=True)
.toTable('orders_delta')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets try and add the third file that has an extra column
# MAGIC

# COMMAND ----------

(order_df.writeStream
.format('delta')
.option('checkpointLocation',checkpointlocation)
.option('mergeSchema','true')
.trigger(availableNow=True)
.toTable('orders_delta')
)

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see that the pipeline failed and we cant even fix it by retrying the readStream so it needs to be fixed by changing the schema or removing the problemetic file.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > **[rescue]**
# MAGIC - stream never fails
# MAGIC - all new columns and the corrupt records are added in rescued column
# MAGIC
# MAGIC > For this demo
# MAGIC - delete the volume files 3 and 4
# MAGIC - delete the checkpoint location
# MAGIC - drop the table
# MAGIC - we will just change the value schemaEvolutionMode to rescue
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_delta;

# COMMAND ----------

order_df=(spark.readStream
          .format('cloudFiles')
          .option('cloudFiles.format','csv')
          .option('cloudFiles.inferSchema','true')
          .option('cloudFiles.inferColumnTypes','true')
          .option('cloudFiles.schemaLocation',checkpointlocation)
          .option('cloudFiles.schemaEvolutionMode','rescue')
          .load(orders))
(order_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .trigger(availableNow=True)
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets add the third file to our volume and see our results.
# MAGIC

# COMMAND ----------

(order_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .trigger(availableNow=True)
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see no fail in our pipeline lets now view the data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see the column added to the rescued column.
# MAGIC > Now lets add the 4th file
# MAGIC

# COMMAND ----------

(order_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .trigger(availableNow=True)
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC We can see the data being added to the rescued column
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > **[addNewColumns]**
# MAGIC - stream fails but works after evolving the schema by running the readStream again
# MAGIC - stream works after the readStream has run
# MAGIC
# MAGIC > For this demo
# MAGIC - delete the volume files 3 and 4
# MAGIC - delete the checkpoint location
# MAGIC - drop the table
# MAGIC - we will just change the value schemaEvolutionMode to addNewColumns if we dont provide mode it is set by default.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if exists orders_delta;

# COMMAND ----------

ordered_df=spark.readStream \
    .format('cloudFiles') \
    .option('cloudFiles.format','csv') \
    .option('cloudFiles.schemaLocation',checkpointlocation) \
    .option('cloudFiles.schemaEvolutionMode','addNewColumns') \
    .option('cloudFiles.inferColumnTypes','true') \
    .option('cloudFiles.inferColumnTypes','true') \
    .load(orders)

ordered_df.writeStream \
    .format('delta') \
    .option('mergeSchema','true') \
    .option('checkpointLocation',checkpointlocation) \
    .trigger(availableNow=True) \
    .toTable('orders_delta')
    
    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC NOW LETS ADD THE THIRD FILE
# MAGIC

# COMMAND ----------

ordered_df.writeStream \
    .format('delta') \
    .option('mergeSchema','true') \
    .option('checkpointLocation',checkpointlocation) \
    .trigger(availableNow=True) \
    .toTable('orders_delta')


# COMMAND ----------

# MAGIC %md
# MAGIC We can see clearly the error and it says to run the readStream again lets run that cell 34 and see the results.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets finally add the 4th file
# MAGIC

# COMMAND ----------

ordered_df.writeStream \
    .format('delta') \
    .option('mergeSchema','true') \
    .option('checkpointLocation',checkpointlocation) \
    .trigger(availableNow=True) \
    .toTable('orders_delta')

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets view the data finally, we will see the rescued_data column that gives us the details for the column causing issue.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC Lets finally view the history
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see there are three stream updates.
# MAGIC
# MAGIC ### IMP-
# MAGIC - the default option when we inferSchema is addNewColumns.
# MAGIC - the defualt option when we define the schema is rescue.