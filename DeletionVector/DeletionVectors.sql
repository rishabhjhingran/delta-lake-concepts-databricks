-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## DELETION VECTOR DEMO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Volume

-- COMMAND ----------

create volume if not exists my_demo_records

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Entering Data to volumne

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a Data Frame Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [
-- MAGIC     (1, "Alice", 25, "Lucknow", 85.5),
-- MAGIC     (2, "Bob", 30, "Indore", 90.0),
-- MAGIC     (3, "Charlie", 35, "Delhi", 78.2),
-- MAGIC     (4, "Diana", 40, "Mumbai", 92.3)
-- MAGIC ]
-- MAGIC
-- MAGIC # Define schema
-- MAGIC columns = ["ID", "Name", "Age", "City", "Score"]
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC
-- MAGIC # Show DataFrame
-- MAGIC df.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ### Create a Sub Folder inside main volume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/Volumes/workspace/default/my_demo_records/records')

-- COMMAND ----------

-- MAGIC %md #### Writing the dataframe into volume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format('delta').mode('overwrite').save('/Volumes/workspace/default/my_demo_records/records/')

-- COMMAND ----------

Select * from delta.`/Volumes/workspace/default/my_demo_records/records/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Adding another 3 datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [
-- MAGIC     (5, "Sarthak", 25, "Lucknow", 85.5),
-- MAGIC     (6, "Rishabh", 30, "Indore", 90.0),
-- MAGIC     (7, "Mahima", 35, "Delhi", 78.2)
-- MAGIC    
-- MAGIC ]
-- MAGIC
-- MAGIC # Define schema
-- MAGIC columns = ["ID", "Name", "Age", "City", "Score"]
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC
-- MAGIC # Show DataFrame
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %md #### Add this record to the existing volume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format('delta').mode('append').save('/Volumes/workspace/default/my_demo_records/records/')

-- COMMAND ----------

-- MAGIC %md ### View your Data

-- COMMAND ----------

Select * from delta.`/Volumes/workspace/default/my_demo_records/records/`

-- COMMAND ----------

-- MAGIC %md #### Delete the Record with ID 4

-- COMMAND ----------

Delete From delta.`/Volumes/workspace/default/my_demo_records/records/` where ID=4

-- COMMAND ----------

-- MAGIC %md ### View your log file

-- COMMAND ----------

Select * from json.`/Volumes/workspace/default/my_demo_records/records/_delta_log/00000000000000000003.json`

-- COMMAND ----------

-- MAGIC %md ### View the history

-- COMMAND ----------

DESCRIBE HISTORY delta.`/Volumes/workspace/default/my_demo_records/records/`

-- COMMAND ----------

-- MAGIC %md #### View the details of the table 

-- COMMAND ----------

DESCRIBE DETAIL delta.`/Volumes/workspace/default/my_demo_records/records/`

-- COMMAND ----------

-- MAGIC %md #### Read the _metadata

-- COMMAND ----------

Select *,_metadata.file_path from delta.`/Volumes/workspace/default/my_demo_records/records/`


-- COMMAND ----------

-- MAGIC %md ### Version Controlling

-- COMMAND ----------

DESCRIBE HISTORY delta.`/Volumes/workspace/default/my_demo_records/records/`

-- COMMAND ----------

-- MAGIC %md ## TIME TRAVELING
-- MAGIC ### GO TO A PARTICULAR VERSION USING AS OF VERSION AND TIMESTAMP

-- COMMAND ----------

-- MAGIC %md #### 1.USING AS OF

-- COMMAND ----------

-- MAGIC %md #### Using AS OF

-- COMMAND ----------

SELECT * FROM delta.`/Volumes/workspace/default/my_demo_records/records/` VERSION AS OF 0


-- COMMAND ----------

-- MAGIC %md #### Using the @v command

-- COMMAND ----------

Select * from delta.`/Volumes/workspace/default/my_demo_records/records/`@v2

-- COMMAND ----------

-- MAGIC %md ### 2.USING TIMESTAMP 

-- COMMAND ----------

-- MAGIC %md ### View the History

-- COMMAND ----------

DESCRIBE HISTORY delta.`/Volumes/workspace/default/my_demo_records/records/`

-- COMMAND ----------

-- MAGIC %md ### Check the timestamp from the history then use it for going to timestamp
-- MAGIC #### Now we will get the version 2 that is the state of the table when we deleted the record with ID 4

-- COMMAND ----------

SELECT * FROM delta.`/Volumes/workspace/default/my_demo_records/records/` TIMESTAMP AS OF '2025-12-02T08:36:13.000+00:00'

-- COMMAND ----------

-- MAGIC %md #### Now we dont need to specify the full timestamp but we can provide the relative time that can fetch the data to the nearest timestamp that is closer to the lower limit , if you see the result we give the relative time or the time format in which we want so that gets as to the first version 

-- COMMAND ----------

SELECT * FROM delta.`/Volumes/workspace/default/my_demo_records/records/` TIMESTAMP AS OF '2025-12-02T08:32'

-- COMMAND ----------

-- MAGIC %md #### RESTORE YOUR CURRENT TABLE TO THE OLDER VERSION

-- COMMAND ----------

-- MAGIC %md #### View the history

-- COMMAND ----------

DESCRIBE HISTORY delta.`/Volumes/workspace/default/my_demo_records/records/`


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Restore the table to the version 1
-- MAGIC ##### RESTORE TABLE table_name VERSION AS OF 1

-- COMMAND ----------

RESTORE TABLE delta.`/Volumes/workspace/default/my_demo_records/records/` TO VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md #### Now we can view the data and the Parquet file that is used to get the results.
-- MAGIC ##### Now if you see in the results the data is generated from the two parquet files that we have used to generate the version 1 that can be seen in the file path it uses the first two parquet files that were written 

-- COMMAND ----------

Select *,_metadata.file_path FROM delta.`/Volumes/workspace/default/my_demo_records/records/`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## DELETION VECTOR WITH REORG AND CONCEPT OF MAKING SURE OUR READ OPERATIONS ARE NOT HINDERED BECAUSE OF DELETION VECTORS 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### First Lets now create a different folder 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/Volumes/workspace/default/my_demo_records/reorg')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lets add the data in the volume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [
-- MAGIC     (1, "Alice", 25, "Lucknow", 85.5),
-- MAGIC     (2, "Bob", 30, "Indore", 90.0),
-- MAGIC     (3, "Charlie", 35, "Delhi", 78.2),
-- MAGIC     (4, "Diana", 40, "Mumbai", 92.3)
-- MAGIC ]
-- MAGIC
-- MAGIC columns = ["ID", "Name", "Age", "City", "Score"]
-- MAGIC
-- MAGIC df_spark = spark.createDataFrame(data, columns)
-- MAGIC df_spark.show()
-- MAGIC df_spark.write.format('delta').mode('overwrite').save('/Volumes/workspace/default/my_demo_records/reorg/')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Now we can find the parquet file and metadata in the reorg volume
-- MAGIC ##### Run the Select query to view the data

-- COMMAND ----------

Select * from delta.`/Volumes/workspace/default/my_demo_records/reorg/`


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Now we can see above we have four records now lets write 3 more records to it and see the logs and parquet file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [
-- MAGIC     (5, "Rishabh", 25, "Lucknow", 85.5),
-- MAGIC     (6, "Sarthak", 30, "Indore", 90.0),
-- MAGIC     (7,'Mahima',45,'Bangalore',89.0)
-- MAGIC     
-- MAGIC ]
-- MAGIC
-- MAGIC columns = ["ID", "Name", "Age", "City", "Score"]
-- MAGIC
-- MAGIC df_spark = spark.createDataFrame(data, columns)
-- MAGIC df_spark.show()
-- MAGIC df_spark.write.format('delta').mode('append').save('/Volumes/workspace/default/my_demo_records/reorg/')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Now Lets try to delete the data with ID 4
-- MAGIC

-- COMMAND ----------

DELETE FROM delta.`/Volumes/workspace/default/my_demo_records/reorg` where ID=4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### If you see now you can find a deletion vector along with your parquet file 
-- MAGIC ##### Now the issue is if we have a file with million records and we want to delete a half a million records then we have multiple delete vectors created that we will make the read operations very slower and will have more cost in reading the delta table.
-- MAGIC #### To Resolve this issue rather than using delta vector we will write the whole file again that will help us in removing the deletion vector and write the new parquet file which will delete the value.
-- MAGIC ##### The Resolution steps for Deletion vector 
-- MAGIC * REORG THE TABLE
-- MAGIC * RUN OPTIMIZE ( BY DEFAULT)
-- MAGIC * VACUUM THE DATA (will delete the unused parquet file from the data [by default 7 days ])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### First Step - REORG

-- COMMAND ----------

REORG TABLE delta.`/Volumes/workspace/default/my_demo_records/reorg` APPLY(PURGE)

-- COMMAND ----------

-- MAGIC %md #### VIEW THE HISTORY OF TABLE

-- COMMAND ----------

DESCRIBE HISTORY delta.`/Volumes/workspace/default/my_demo_records/reorg`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Now the Optimize command will make the parquet file that is going to use 

-- COMMAND ----------

-- MAGIC %md #### Set Table Property to be able to delete files that are less than 7 days using vaccum 
-- MAGIC ##### We will alter the table to fix the issue

-- COMMAND ----------

ALTER TABLE delta.`/Volumes/workspace/default/my_demo_records/reorg` SET TBLPROPERTIES (delta.deletedFileRetentionDuration= 'interval 0 hours')

-- COMMAND ----------

-- MAGIC %md #### Now we will Vacuum data (First we will dry run -- this will give us the files that we are going to delete)

-- COMMAND ----------

VACUUM delta.`/Volumes/workspace/default/my_demo_records/reorg` DRY RUN

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Now We will perform VACUUM 

-- COMMAND ----------

VACUUM delta.`/Volumes/workspace/default/my_demo_records/reorg`
