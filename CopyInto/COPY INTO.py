# Databricks notebook source
# MAGIC %md
# MAGIC > COPY INTO 
# MAGIC - It loads the data from cloud file location into delta lake.
# MAGIC - This is retryable and indempotent operation.
# MAGIC - Exactly once processing.
# MAGIC
# MAGIC INCREMENTAL BATCH - files in the source location that have already loaded are skipped.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets go through with the example for that I will create a new volume COPY_INTO
# MAGIC - that we will use to create a order folder inside the volume and load the data into it 

# COMMAND ----------

# MAGIC %sql
# MAGIC Create VOLUME if not exists copy_into;

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/workspace/default/copy_into/orders

# COMMAND ----------

# MAGIC %md
# MAGIC NOW LETS UPLOAD THE FIRST FILE INTO OUR ORDERS FOLDER FROM PATH - E:\Databricks Labs\4515050-Week5-Datasets-2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### We will have 2 USE CASES for this scenario ---
# MAGIC #### **SCENARIO 1**
# MAGIC > Lets create the orders table without defining any schema to it 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists orders;
# MAGIC create table orders;

# COMMAND ----------

# MAGIC %md
# MAGIC WE WILL USE COPY_INTO statement 

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',')
# MAGIC copy_options --- this function is used to give properties to the target table(orders)
# MAGIC ('mergeSchema'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC WE CAN CLEARLY SEE THE 4 RECORDS , NOW LETS VIEW THE DATA TYPES

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED orders;

# COMMAND ----------

# MAGIC %md
# MAGIC > WE CAN CLEARLY SEE ABOVE THAT IT CONVERTED EVERYTHING INTO STRING.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders;

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see in the history two operatiopns being performed where one is create table and second is COPY INTO
# MAGIC
# MAGIC > Now lets add another file into the volume and check 

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',')
# MAGIC copy_options --- this function is used to give properties to the target table(orders)
# MAGIC ('mergeSchema'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC NOW WE CAN CLEARLY SEE THAT THERE ARE 8 records in the table and we didnot have to create a new table again and it added the data incrementally lets view that in history.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders;

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see two copy_into operations and in the parameters we can find statsOnLoads= True
# MAGIC - How does it remember whart it has processed
# MAGIC   - it creates a copy into log where it maintains the history of all the made changes.
# MAGIC   - you can find it in _delta _log folder.
# MAGIC     - _copy_into_ -- file that we cant read

# COMMAND ----------

# MAGIC %md 
# MAGIC Lets add the 3rd file and view the data 
# MAGIC - in the third file we have an extra column

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',')
# MAGIC copy_options --- this function is used to give properties to the target table(orders)
# MAGIC ('mergeSchema'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md 
# MAGIC We can clearly see that we have 12 rows and the schema has evolved to add the extra column

# COMMAND ----------

# MAGIC %md
# MAGIC Now LETS add the 4th file with corrupt record for customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',')
# MAGIC copy_options --- this function is used to give properties to the target table(orders)
# MAGIC ('mergeSchema'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly view that the data is added because we have not defined schema and it is taking everything in the string.
# MAGIC
# MAGIC - The schema evolution happens because we have used copy_option (mergeSchema=true),if we remove that we will receive an error.
# MAGIC - lets depict that using an example by following the below steps.
# MAGIC   - deleted all the data from volume/orders.
# MAGIC   - drop  the table.
# MAGIC   - create the table.
# MAGIC   - copy into without using copy_option

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists orders;
# MAGIC create table orders;

# COMMAND ----------

# MAGIC %md
# MAGIC UPLOAD THE FIRST FILE INTO VOLUME
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC WE CAN CLEARLY SEE THE [COPY_INTO_SCHEMA_MISMATCH_WITH_TARGET_TABLE] because we didnot specify any schema while we were creating the table and the data we were trying to write had a schema thats why copy_option is required for icremental data load.
# MAGIC
# MAGIC > Now lets use the test case where we create the table with copy_option and then remove it after 1st operation 

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',')
# MAGIC copy_options --- this function is used to give properties to the target table(orders)
# MAGIC ('mergeSchema'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets add the second file to the volume and run the statement without adding copy_option

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC WE CAN CLEALRY SEE IT DIDNOT GIVE ERROR BECAUSE THE SCHEMA WAS THE SAME.
# MAGIC
# MAGIC > NOW LETS ADD THE THIRD FILE WITH THE SCHEMA EVOLUTION

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC WE CAN CLAERLY SEE THE ERROR [COPY_INTO_SCHEMA_MISMATCH_WITH_TARGET_TABLE] SO THATS WHY COPY_OPTION is important so that there is no issue with the target table.

# COMMAND ----------

# MAGIC %md 
# MAGIC COPY_OPTION 
# MAGIC > CASE 3 where we infer the schema 

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true',
# MAGIC 'mergeSchema'='true', -- merge the schema of the source files.
# MAGIC 'delimiter'=',',
# MAGIC 'inferSchema'='true')
# MAGIC copy_options --- this function is used to give properties to the target table(orders)
# MAGIC ('mergeSchema'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC WE CAN CLEARLY SEE WE GET [DELTA_FAILED_TO_MERGE_FIELDS] 
# MAGIC because the initial data type was string.
# MAGIC
# MAGIC - Copy into is best suited when we are dealing with upto 1000 files.
# MAGIC - Lets us to perform incremental data load.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCENARIO 2**
# MAGIC > _Copy Into with schema _

# COMMAND ----------

# MAGIC %md
# MAGIC DROP THE EXISTING TABLE AND REMOVE EVERYTHIONG FROM THE VOLUME

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders;
# MAGIC create table orders(
# MAGIC   order_id int,
# MAGIC   order_date string,
# MAGIC   customer_id int,
# MAGIC   order_status string
# MAGIC )using delta;

# COMMAND ----------

# MAGIC %md
# MAGIC Load the first file again to the volume

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from (select 
# MAGIC cast(order_id as int) as order_id,
# MAGIC order_date,
# MAGIC cast(customer_id as int) as customer_id,
# MAGIC order_status
# MAGIC from '/Volumes/workspace/default/copy_into/orders')
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC NOW SINCE WE ARE DEFINING THE STRUCTURE OF THE TABLE ALREADY.
# MAGIC SO WE DONT NEED TO USE mergeSchema IN FORMAT_OPTION AS WELL AS [COPY_OPTIONS] BECAUSE SCHEMA IS ALREADY DEFINED IN CREATE ORDER.
# MAGIC
# MAGIC >Now lets add the second file 

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from (select 
# MAGIC cast(order_id as int) as order_id,
# MAGIC order_date,
# MAGIC cast(customer_id as int) as customer_id,
# MAGIC order_status
# MAGIC from '/Volumes/workspace/default/copy_into/orders')
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC NOW we can clearly see it added the second file because it didnot have any changes and we can view 8 rows.
# MAGIC  > Lets add the third file now

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from (select 
# MAGIC cast(order_id as int) as order_id,
# MAGIC order_date,
# MAGIC cast(customer_id as int) as customer_id,
# MAGIC order_status
# MAGIC from '/Volumes/workspace/default/copy_into/orders')
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC NOW WE CAN CLEARLY SEE THEIR IS NO ERROR SINCE WE ALREADY HAVE A SCHEMA DEFINED AND IT WILL TAKE THE COLUMNS REQUIRED USING OUR SUBQUERY WE CREATED AND ADD THE RECORDS FOR THE DEFINED COLUMNS.
# MAGIC
# MAGIC > BUT NOW LETS TEST THE SCENARIO WE HAVE USED SELECT *

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders 
# MAGIC from '/Volumes/workspace/default/copy_into/orders'
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC We receive [DELTA_FAILED_TO_MERGE_FIELDS] error because it is of different type

# COMMAND ----------

# MAGIC %md
# MAGIC LETS GO BACK TO THE ORIGINAL QUERY AND THEN ADD FILE 4.

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from (select 
# MAGIC cast(order_id as int) as order_id,
# MAGIC order_date,
# MAGIC cast(customer_id as int) as customer_id,
# MAGIC order_status
# MAGIC from '/Volumes/workspace/default/copy_into/orders')
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true');
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders;

# COMMAND ----------

# MAGIC %md
# MAGIC Even though we have used only the selected columns we still see the error [CAST_INVALID_INPUT] because customer_id was int type and file 4 provided a string type value and if we view the history we can only find 3 copy into statements entry for the three sucessfull test cases.

# COMMAND ----------

# MAGIC %md
# MAGIC WE CAN ALSO ADD META DATA AND OTHER DETAILS TO THE TABLE SCHEMA DEFINITION AND USE THEM.
# MAGIC
# MAGIC - Lets first drop the table and create it again

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE if exists orders;
# MAGIC create table orders(
# MAGIC   order_id int,
# MAGIC   order_date string,
# MAGIC   customer_id int,
# MAGIC   order_status string,
# MAGIC   file_name string,
# MAGIC   load_time timestamp
# MAGIC
# MAGIC )using delta;

# COMMAND ----------

# MAGIC %md
# MAGIC NOW DROP THE 4th FILE FROM THE VOLUME AND RUN THE COPY INTO STATEMENT.

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders from (select 
# MAGIC cast(order_id as int) as order_id,
# MAGIC order_date,
# MAGIC cast(customer_id as int) as customer_id,
# MAGIC order_status,
# MAGIC _metadata.file_name as file_name,
# MAGIC current_timestamp() as load_time
# MAGIC from '/Volumes/workspace/default/copy_into/orders')
# MAGIC fileformat=CSV --- format of the file
# MAGIC format_options --- this function is used to give properties to the source files
# MAGIC ('header'='true');
# MAGIC select * from orders;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see the file_name and load_time being added.
# MAGIC
# MAGIC - COPY into --- incremental data load but limited number of files.
# MAGIC - We will use auto loader for more files and better loading.
# MAGIC - copy into is not recomended anymore to use.