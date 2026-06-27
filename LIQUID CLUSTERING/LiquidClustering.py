# Databricks notebook source
# MAGIC %md
# MAGIC > CHALLENGES WITH PARTITIONING AND ZORDERING -
# MAGIC - We have to struggle to determine the partitioning column and zorder column.
# MAGIC - If the reading pattern changes then we have to rewrite all the data.
# MAGIC
# MAGIC > LIQUID CLUSTERING 
# MAGIC - Lets understand the scenario where we have multiple files 
# MAGIC   - 2020 -small
# MAGIC   - 2021 - small
# MAGIC   - 2022 - small
# MAGIC   - 2023 - medium
# MAGIC   - 2024 - small 
# MAGIC   - 2025 - small 
# MAGIC - We will have small file problems.
# MAGIC - So we would ideally want to have a medium file .
# MAGIC - This will be done by combining two small into 1 medium.
# MAGIC Lets consider a scenario where we have partitioning over country and the date -- us 04012025 has avery small file and india 05012025 also has a very small file.
# MAGIC - If you would have gone for partitioning with country and date for country with a poarticular date we would have received small file problem thats where liquid clustering will help us solve this problem.
# MAGIC - It also helps us in doing incremental data load.

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets Now use the same concept to get the dataset from databricks
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Create volume if not exists liquid_clustering_demo

# COMMAND ----------

table_location='dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/'
all_files=[f.path for f in dbutils.fs.ls(table_location) if f.name.endswith('.parquet')]
files_10=all_files[:10]
print('using these 10 files',files_10)
df_10=spark.read.parquet(*files_10)
display(df_10)
volume_path = "/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi/"

df_10.repartition(200).write.format("delta") \
  .mode("overwrite") \
  .option("delta.autoOptimize.optimizeWrite", "false") \
  .option("delta.autoOptimize.autoCompact", "false") \
  .save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the data now 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets add the cluster column

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi` cluster by (trip_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets check the history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see that the data is added successfully .
# MAGIC - > But is just added it is not executed to execute this we will have to run the optimize command if we have disabled the auto optimize like in our case .
# MAGIC
# MAGIC >Lets run the optimize command

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md
# MAGIC Lets see the details of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see the clusteringColumns trip_distance 

# COMMAND ----------

# MAGIC %md
# MAGIC - Again we will have give the column.So we have a feature in liquid clustering to auto enable the liquid clustering where over time it will smartly choose which column to choose.
# MAGIC
# MAGIC > Lets remove the existing clustering and then view the data 
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi/`
# MAGIC CLUSTER BY NONE

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets describe the table again 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see that there is no column mentioned by clusteringColumns

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets add auto clustering feature now

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`
# MAGIC CLUSTER BY AUTO

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets describe the table again

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md
# MAGIC We dont see any option because we havent run the optimize command since we have to run optimize statement to execute the clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md
# MAGIC We will have to go to version 0 to try auto clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`
# MAGIC TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC Lets add the auto clustering now

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`
# MAGIC CLUSTER BY AUTO

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > View the details 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %md 
# MAGIC It will do clustering on the fly with the updates using auto cluster to improve query performance 

# COMMAND ----------

# MAGIC %md
# MAGIC ## LETS TRY THE SCENARIO WHERE WE DO PARITIONING AND ZORDERING TOGETHER
# MAGIC > Restore the table to version 0 and see properties

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi` VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/liquid_clustering_demo/nyc_taxi` ADD PARTITION(vendor_id)

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see that we cant create partitions over already created table so we will have to create another table for that lets create a volume and repeat the above steps

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Volume if not exists final_demo_delta_lake_concepts;

# COMMAND ----------

table_location='dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/'
all_files=[f.path for f in dbutils.fs.ls(table_location) if f.name.endswith('.parquet')]
files_10=all_files[:10]
print('using these 10 files',files_10)
df_10=spark.read.parquet(*files_10)
display(df_10)
volume_path = "/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo/"

df_10.repartition(200).write.format("delta") \
  .mode("overwrite") \
  .partitionBy('vendor_id') \
  .option("delta.autoOptimize.optimizeWrite", "false") \
  .option("delta.autoOptimize.autoCompact", "false") \
  .save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see that the partitions are created in our delta table with the vendor id.
# MAGIC > Now if you check closely you can find that there is skewness in the table sinze vendor 1 has only 1 file.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from delta.`/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo/` where trip_distance>200;

# COMMAND ----------

# MAGIC %md
# MAGIC > If we see the query performance now we can clearly see that it had read 2 partitions and 89 files to get the results.

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets try to zorder by trip_distance

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo/` zorder by(trip_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC We can see the number of files added are 12 number of files removed 600
# MAGIC > Now lets run the same query again

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from delta.`/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo/` where trip_distance>200;

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see a performance improvement as it reads only 2 files 
# MAGIC -  It cannot club from different partitions.
# MAGIC - It still has to go through different partitions
# MAGIC - Now lets check overlapping
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(trip_distance),max(trip_distance),_metadata.file_name from delta.`/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo/`
# MAGIC group by _metadata.file_name order by min(trip_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC > We can still see some overlapping due to partitions , now lets try the same thing by using clustering

# COMMAND ----------

table_location='dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/'
all_files=[f.path for f in dbutils.fs.ls(table_location) if f.name.endswith('.parquet')]
files_10=all_files[:10]
print('using these 10 files',files_10)
df_10=spark.read.parquet(*files_10)
display(df_10)
volume_path = "/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo2/"

df_10.repartition(200).write.format("delta") \
  .mode("overwrite") \
  .clusterBy(['vendor_id','trip_distance']) \
  .option("delta.autoOptimize.optimizeWrite", "false") \
  .option("delta.autoOptimize.autoCompact", "false") \
  .save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can clearly see that we can to cluster on two columns 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo2/`

# COMMAND ----------

# MAGIC %md
# MAGIC We can find the two cluster columns here now lets run the min max query again to see how it repsonds
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(trip_distance),max(trip_distance),_metadata.file_name from delta.`/Volumes/workspace/default/final_demo_delta_lake_concepts/nyc_taxi_demo2/`
# MAGIC group by _metadata.file_name order by min(trip_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see in performance the results are abit faster and the query is optimized 

# COMMAND ----------

# MAGIC %md
# MAGIC ## THEORY
# MAGIC - Incremental clustering-- write only newer data as it arrives.
# MAGIC - flexibility to change the column (auto)
# MAGIC - avoid rewriting the data 
# MAGIC
# MAGIC > Issue with Partitioning-
# MAGIC - Merge conflicts happenbecause we are working on the same partition.
# MAGIC
# MAGIC Liquid Clustering solves this as we are working at row level so the merge conflicts dont happen.
# MAGIC
# MAGIC > BENEFITS OF LIQUID CLUSTERING -
# MAGIC - tables often filtered by high cardinality.
# MAGIC - tables with significant skew in data distribution.
# MAGIC - tables with access patterns that change over time.
# MAGIC - tables wioth concurrent write requirements
# MAGIC - tables that require a lot of maintenance and tuning efforts
# MAGIC - you can enable liquid clustering on a brand new table or an existing table.
# MAGIC - after liquid clustering is enabled run optimize jopbs as usual to incrementally cluster new data.
# MAGIC - if yopu have predictive optimization enabled optimize runs automatically.
# MAGIC
# MAGIC > Ideal Data Layout-
# MAGIC - files between 16MB to 1GB.
# MAGIC - no overlapping data ranges
# MAGIC - cost of maintaing the data layout must be cheap

# COMMAND ----------

# MAGIC %md
# MAGIC #### PREDICTIVE OPTIMIZATION
# MAGIC - It removes the need to manually manage the maintenance operations for managed tables on azure databricks.
# MAGIC - optimize - triggers incremental clustering for liquid enabled tables,improves query performance by ioptimizing file sizes.
# MAGIC - vacuum - reduces storage costs by deleting data files no longer referenced by the table.
# MAGIC - analyze - triggers incremental update of stats to improve query performance.
# MAGIC
# MAGIC SERVERLESS compute for these background jobs ,your account is billed for compute associated with these workloads using a serverless jobs sku.