# Databricks notebook source
# MAGIC %md
# MAGIC > In this demo we will try to use streaming mode lets try it in serverless mode 
# MAGIC STEPS -
# MAGIC - delete the files from the volume.
# MAGIC - delete the checkpoint 
# MAGIC - drop the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_delta

# COMMAND ----------

landingfilepath='/Volumes/workspace/default/autoloader'
orders=landingfilepath+'/orders'
checkpointlocation=landingfilepath+'/orders_checkpoint'

# COMMAND ----------

ordered_df=(spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format','csv')
            .option('cloudFile.inferSchema','true')
            .option('cloudFiles.ColumnTypes','true')
            .option('cloudFiles.schemaLocation',checkpointlocation)
            .load(orders))
# Add the file and then do write stream
(ordered_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(processingTime='10 seconds')
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can clearly see that in serverless cluster we donot have the option for processingTime so lets just understand how it would work .
# MAGIC
# MAGIC > ProcessingTime='10 seconds'
# MAGIC - it will run the writeStream and check for the files every 10 seconds and if it finds the file it will write the data into the delta table.
# MAGIC - for real time streaming we can shorter the time interval even more.
# MAGIC - it will continously rerun according to our given duration until we manually stop the process.
# MAGIC - it will also stop when we try to add the orders3.csv where we have an extra column and will ask you to rerun the readStream to evolve the schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### THEORY
# MAGIC 1. How Schema Inference Works internally.
# MAGIC   - autoloader reads 
# MAGIC       - first 1000 files or 50GB of data whichever comes first.
# MAGIC       - even if 1000 files of 1mb is read it will update using that,similarly if there are 5 files of 10GB each it will read the 5 files.
# MAGIC       - in our case we just have 1 file first so it will read it and inferSchema.
# MAGIC   - Properties that control these settings -
# MAGIC     - cloudFiles.schema.Inference.sampleSize.minBytes
# MAGIC     - cloudFiles.schema.Inference.sampleSize.numFiles

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets define our schema and see what happens

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table orders_delta;

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType
schema=StructType([
  StructField('order_id',IntegerType(),True),
  StructField('order_date',TimestampType(),True),
  StructField('customer_id',IntegerType(),True),
  StructField('order_status',StringType(),True)
])
ordered_df=(spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format','csv')
            .schema(schema)# the defined schema
            .option('header','true')# since we dont use inferSchema we have to tell that csv has headers
            .option('cloudFiles.schemaLocation',checkpointlocation)
            .option('rescuedDataColumn','corrupt_records')
            .load(orders)
)
(ordered_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta')
 )



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see the first file being added now lets add the second file and then view the data 

# COMMAND ----------

(ordered_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta')
 )

# COMMAND ----------

# MAGIC %md
# MAGIC Lets view the data and then history
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see it shows two streming updates that we have made.
# MAGIC > Now lets add the third file.
# MAGIC - that file has an extra column.
# MAGIC - when we ran the inferSchema we found that it failed.
# MAGIC
# MAGIC Lets see what happens when we have already added the schema
# MAGIC

# COMMAND ----------

(ordered_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta')
 )

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see that it doesnot give any errors since we had already defined our own schema now lets view the data .
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from orders_delta;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see it added the extra collumn to the rescued data column that we defined while reading the stream(corrupt_records)
# MAGIC Now if we add the 4th file we will find the corrupt record in the same column
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### THEORY
# MAGIC 2. Schema Hints
# MAGIC -   STEPS-
# MAGIC     - drop the table
# MAGIC     - delete all the files 
# MAGIC     - delete checkpoint 
# MAGIC     - infer the schema 
# MAGIC   
# MAGIC   The concept of Schema hints is lets say we have a customer_id that is INT for time being but over time it may be taking BIGINT as the data type thats where schema hints us gives us the option to be ready forsuch changes so that our stream and pipeline doesnot break when we are using it in production.
# MAGIC
# MAGIC   > Lets see with the help of demo , do the above steps.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_delta;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC Lets read the stream
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp
order_df=(spark.readStream
.format('cloudFiles')
.option('cloudFiles.format','csv')
.option('cloudFiles.inferSchema','true')
.option('cloudFiles.inferColumnTypes','true')
.option('cloudFiles.schemaLocation',checkpointlocation)
.option('cloudFiles.schemaHints','order_id integer,order_date string')# We will make sure that order_id is BIGINT and order_date is STRING.
.load(orders)
.withColumn('file_name',col('_metadata.file_name'))# Add a column with file name
.withColumn('loadtime',current_timestamp()) # Add a column with current timestamp
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets write the data to the stream
# MAGIC

# COMMAND ----------

(order_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see above that we used to two files in the orders volume that is mentioned in the file_name 
# MAGIC
# MAGIC > Now lets add the third file and perform a write 
# MAGIC

# COMMAND ----------

(order_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta')
 )

# COMMAND ----------

# MAGIC %md
# MAGIC We receive the same error for schema evolution lets run the readStream again and then try writeStream
# MAGIC

# COMMAND ----------

# After running the readStream running writeStream
(order_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta')
 )

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see that the stream worked now lets view the data and then add 4th file -- write stream --- view the data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta

# COMMAND ----------

(order_df.writeStream
 .format('delta')
 .option('mergeSchema','true')
 .option('checkpointLocation',checkpointlocation)
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC WE CAN CLEALRY SEE THAT THE CORRUPT DATA IS ADDED TO THE _RESCUED_DATA COLUMN.