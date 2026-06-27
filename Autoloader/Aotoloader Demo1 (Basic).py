# Databricks notebook source
# MAGIC %md
# MAGIC SCENARIOS WHICH COPY INTO CANT HANDLE -
# MAGIC - file coming every 5min,2 mins or even more frequently 
# MAGIC - batch job is not suitable for such high frequency
# MAGIC - if millions of new files are coming each hour or you have to backfile(load previous records before new ones) billions of files,COPY INTO only suitable for 1000 files.
# MAGIC
# MAGIC > TO SOLVE THESE PROBLEMS WE HAVE AUTOLOADER
# MAGIC - more advanced technique then copy into to incrementaly & efficiently process the new data files as they arrive in cloud storage.
# MAGIC - autoloader can handle both batch and streaming.
# MAGIC - you can use autoloader to p;rocess billions of files to migrate or backfile a table.
# MAGIC - autoloader scales to support near real time ingestion of millions of files per hour.
# MAGIC - can load files from any cloud source -- 
# MAGIC     amazon s3
# MAGIC     adls gen2
# MAGIC     gcs

# COMMAND ----------

# MAGIC %md 
# MAGIC Lets look into the demo 
# MAGIC > We can create streaming tables in sql without pipeline hence we will use pyspark.
# MAGIC STEPS -
# MAGIC - Create a volume
# MAGIC - Create a folder 
# MAGIC - Create a checkpoint folder (checkpoint is used to incrementally load the data and if there is any failure the job knows from where it needs to load the data)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS AUTOLOADER;

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/workspace/default/autoloader/orders
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Create variables that can be used throughout our demo

# COMMAND ----------

landing_zone='/Volumes/workspace/default/autoloader'
orders_file=landing_zone+'/orders'
checkpoint_file=landing_zone+'/orders_checkpoint'

# COMMAND ----------

# MAGIC %md
# MAGIC WHEN WE READ STREAM OR USE AUTOLOADER WE USE readStream so that we know it is an autoloader 
# MAGIC
# MAGIC > Now lets create a new readstream

# COMMAND ----------

orders_df=(spark.readStream
            .format('cloudFiles')#for autoloader to come into picture we have to use [cloudFiles]
            .option('cloudFiles.format','csv')
            .option('cloudFiles.inferSchema','true')#infer the columns i.e number of columns
            .option('cloudFiles.schemaLocation',checkpoint_file)#schema will be tracked for schema evolution
            .option('cloudFiles.inferColumnTypes','true')#For csv,xml,json we have to give column types else it will take as string
            .load(orders_file)
            )

# COMMAND ----------

# MAGIC %md
# MAGIC Now after getting the dataframe we would want to convert it into delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists orders_delta;

# COMMAND ----------

(orders_df.writeStream
 .format('delta')
 .option('checkPointLocation',checkpoint_file)# it knows where to start if the fail update happens
 .option('mergeSchema','true')
 .outputMode('append')
 .trigger(availableNow=True)
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %md 
# MAGIC We Received the error becasuse we havent added any file to the volume path, if you see we have used inferSchema = true while reading the stream so its mandatory to add the first file before running the writeStream
# MAGIC
# MAGIC > Lets add the orders1.csv in our created volume from E:\Databricks Labs\4515050-Week5-Datasets-2 and write the stream again

# COMMAND ----------

(orders_df.writeStream
 .format('delta')
 .option('checkpointLocation',checkpoint_file)
.option('mergeSchema','true')
 .outputMode('append')
 .trigger(availableNow=True)# trigger to run the streaming write operation availableNow means when we want to run it.
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %md
# MAGIC LETS RUN THE SELECT QUERY "

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC > writeStream is a unified API for both batch and streaming.
# MAGIC - For Stream - trigger(processingTime='10 seconds') runs every 10 seconds
# MAGIC - For Batch - trigger(availableNow=True) runs when we manually run the cell

# COMMAND ----------

# MAGIC %md
# MAGIC - Now Lets add one more file and run the writeStream cell - 13
# MAGIC - Then view the results using select query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC We can see the second file content is also written and we didnt have to run the stream query because we have used cloudFiles and autoloader so it continously monitors the location for new file when we run the writeStream query.
# MAGIC
# MAGIC > Lets add the third file and then witeStream again to view 
# MAGIC - Third file has an extra column orders_amount

# COMMAND ----------

(orders_df.writeStream
 .format('delta')
 .option('checkpointLocation',checkpoint_file)
.option('mergeSchema','true')
 .outputMode('append')
 .trigger(availableNow=True)# trigger to run the streaming write operation availableNow means when we want to run it.
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %md 
# MAGIC We can clearly see the error says [UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE] that means we have a column that is not present in the readStream schema.
# MAGIC - We can see it clearly provides the solution to run the readStream again that way the schema will evolve accordingly.
# MAGIC
# MAGIC - Whenever a new file arrives with a different column we would have to rerun the readStream again

# COMMAND ----------

# MAGIC %md 
# MAGIC LETS RUN THE READSTREAM AGAIN AND THEN WRITE STREAM

# COMMAND ----------

orders_df=(spark.readStream
            .format('cloudFiles')#for autoloader to come into picture we have to use [cloudFiles]
            .option('cloudFiles.format','csv')
            .option('cloudFiles.inferSchema','true')#infer the columns i.e number of columns
            .option('cloudFiles.schemaLocation',checkpoint_file)#schema will be tracked for schema evolution
            .option('cloudFiles.inferColumnTypes','true')#For csv,xml,json we have to give column types else it will take as string
            .load(orders_file)
            )
(orders_df.writeStream
 .format('delta')
 .option('checkpointLocation',checkpoint_file)
.option('mergeSchema','true')
 .outputMode('append')
 .trigger(availableNow=True)# trigger to run the streaming write operation availableNow means when we want to run it.
 .toTable('orders_delta'))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC  We can clearly see the data is added.
# MAGIC  > Now lets add 4th file with data type mismatch and then see how it reacts
# MAGIC   - We can clearly see that we have _rescued_data column for our data

# COMMAND ----------

(orders_df.writeStream
 .format('delta')
 .option('checkpointLocation',checkpoint_file)
.option('mergeSchema','true')
 .outputMode('append')
 .trigger(availableNow=True)# trigger to run the streaming write operation availableNow means when we want to run it.
 .toTable('orders_delta'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC - The job didnot give us any error and added the corrupt column value into the _rescued_data column
# MAGIC - The job fails only when there is a new column added because we have a new column stored at the schemaLocation so we will have to rerun readStream.
# MAGIC - But in the case of wrong column data type value we have no fail because it will automatically add the column  to the _rescued_data column
# MAGIC
# MAGIC > TRACKING
# MAGIC - We track the changes using the checkpoint location.
# MAGIC - We can track all the things.
# MAGIC
# MAGIC > HOW DOES AUTOLOADER TRACK INGESTION PROCESS
# MAGIC - As new files are discovered there metadata is perssisted in a scalable key value store(RocksDB[Very fast keyvalue store])
# MAGIC - we can find it in checkpoint location.
# MAGIC - this key value ensures that data is processed exactly once.
# MAGIC - In case of failure autoloader can resume from where it left off by information stored in checkpoint location.
# MAGIC - This is what helps us in getting exactly once semantics.
# MAGIC - If you delete checkpoint location everything is loast and we have to start a fresh.
# MAGIC - Streaming Mode is not supported in the serverless mode.

# COMMAND ----------

