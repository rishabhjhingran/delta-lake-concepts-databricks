# Databricks notebook source
# MAGIC %md
# MAGIC > It locates the similar into a file.
# MAGIC
# MAGIC - Lets say there is a table where we have the details for yellow_taxi there we have a trip_distance table 
# MAGIC   - trip_distance>50 and trip_distance< 100 , this is our scenario.
# MAGIC   - now we might have to read all the files to fetch this data due to overlap of our data 
# MAGIC   - By Overlap we mean data 1 0-50 , data2 0-100 we can clearly see there is an overlap of data that is 0-50 again in data 2 which is not an ideal condition and will cost us more time ot run the query and more lookup operations in file that will increase the cost as well.
# MAGIC   - Thats where zordering comes into picture --- it sorts the data & then give the files where overlaps are minimum or not there.
# MAGIC   - after zorder file 1 0-50 file 2 50 -100 , this is how zordering works by sorting the files internally & will rearrange it to divide data to prevent overlapping.
# MAGIC
# MAGIC > Now we will demonstrate this using an example -

# COMMAND ----------

# MAGIC %md
# MAGIC > We will use the dataset that is already present in databricks for this demonstration

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/tables/nyctaxi_yellow/

# COMMAND ----------

# MAGIC %md
# MAGIC > Copy the location - dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/
# MAGIC
# MAGIC > Create a volume for your demonstration

# COMMAND ----------

# MAGIC %sql
# MAGIC Create volume if not exists zordering_demo

# COMMAND ----------

# MAGIC %md
# MAGIC > Create a delta table using Volume

# COMMAND ----------

table_location='dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/'
all_files=[f.path for f in dbutils.fs.ls(table_location) if f.name.endswith('.parquet')]
files_10=all_files[:10]
print('using these 10 files',files_10)
df_10=spark.read.parquet(*files_10)
display(df_10)
volume_path = "/Volumes/workspace/default/zordering_demo/nyc_taxi/"

df_10.repartition(200).write.format("delta") \
  .mode("overwrite") \
  .option("delta.autoOptimize.optimizeWrite", "false") \
  .option("delta.autoOptimize.autoCompact", "false") \
  .save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the file path 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select distinct _metadata.file_path from delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi/`

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see it is going to read all 200 files now lets check some scenarios for better understanding

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` where trip_distance > 100;
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we check the query history and go under performance we can find that it has read 134 files and pruned 66 files 

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets run another query to view the overlapping of data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(trip_distance),max(trip_distance),_metadata.file_path from delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` group by _metadata.file_path order by min(trip_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC > If we see above we can clearly see that there is huge overlapping of data in the file ,
# MAGIC  lets do the zordering using optimize
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` ZORDER BY (trip_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC > We can see it removed 200 files and left us with only 11 files because we used zordering on a column.

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets the query to find the overlapping !

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(trip_distance),max(trip_distance),_metadata.file_path from delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` group by _metadata.file_path order by min(trip_distance)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see that there is no overlapping of data and read only 11 files , this will help us in getting the results quickly , lets run the query to find count(*) where trip_distance>100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` where trip_distance > 100;

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see that it read only 1 file that is the power of zordering so that it only has to read 1 file.

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the history
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets go back to our original version and just run optimize 

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` TO VERSION AS OF 0;
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` ;

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see that after running just optimize we just get 12 file rather in zordering we were getting 11 files because it was also using relative data mapping to remove data overlapping while in optimize it just makes file that are of a particular min and max sizze

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets view the files used we can see clearly the data being read from which files.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets run the count(*) again

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from delta.`/Volumes/workspace/default/zordering_demo/nyc_taxi` where trip_distance>100;

# COMMAND ----------

# MAGIC %md
# MAGIC > If we see clearly now in the performance we can clearly see that it had to read 12 files with optimize thats where zorder helps us in reading less files and give us the results faster

# COMMAND ----------

# MAGIC %md
# MAGIC ### THEORY -
# MAGIC - > Zordering:- technique to colocate related information in the same set of files.
# MAGIC - > this colocality is automatically used by delta lake on databricks for data skipping.
# MAGIC - > If you expect a column to be commomly used in predicates & that has high cardinality then use zorder by.
# MAGIC - > Partitioning(Low Cardinality column) --- ZORDERING(High Cardinality Column)
# MAGIC - > Zordering is always donw with optimize
# MAGIC - > We have to create the table again to repartition or change the column for partitioning or zordering , thats a huge drawback.
# MAGIC
# MAGIC THATS WHERE DATABRICK GIVES US THE OPTION TO FLUEDLY CONVERT COLUMNS TO QUERY OPTIMIZATION USING LIQUID CLUSTERING.

# COMMAND ----------

