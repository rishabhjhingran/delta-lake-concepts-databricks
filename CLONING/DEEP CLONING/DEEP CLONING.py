# Databricks notebook source
# MAGIC %md
# MAGIC ## 13-Dec-2025

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEEP CLONING
# MAGIC - > It copies metadata along with the data
# MAGIC - > It is process heavy,takes more storage and takes more time ot complete.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets Create a managed table and perform the following operations on it
# MAGIC - Create the table
# MAGIC - Insert 5 records
# MAGIC - Insert 3 more records
# MAGIC - Update the records

# COMMAND ----------

# MAGIC %sql
# MAGIC ----- create a managed table orders_managed
# MAGIC DROP TABLE IF EXISTS orders_managed;
# MAGIC CREATE OR REPLACE TABLE orders_managed (
# MAGIC   order_id BIGINT,
# MAGIC   sku STRING,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   qty INT,
# MAGIC   unit_price DECIMAL(10,2)
# MAGIC );
# MAGIC ---- add constraints
# MAGIC ALTER TABLE orders_managed 
# MAGIC ADD CONSTRAINT chk_qty CHECK (qty > 0);
# MAGIC ALTER TABLE orders_managed 
# MAGIC ADD CONSTRAINT chk_unit_price CHECK (unit_price > 0);
# MAGIC ----- insert 5 records
# MAGIC INSERT INTO orders_managed VALUES
# MAGIC (1, 'SKU001', 'Product A', 'Category X', 10, 10.00),
# MAGIC (2, 'SKU002', 'Product B', 'Category Y', 5,25.00),
# MAGIC (3, 'SKU003', 'Product C', 'Category Z', 15,27.00),
# MAGIC (4, 'SKU004', 'Product D', 'Category A', 25,295.00),
# MAGIC (5, 'SKU005', 'Product E', 'Category B', 2,24.00);
# MAGIC ----- insert 3 more records
# MAGIC INSERT INTO orders_managed VALUES
# MAGIC (6, 'SKU006', 'Product F', 'Category C', 100, 100.00),
# MAGIC (7, 'SKU007', 'Product G', 'Category D', 50,45.00),
# MAGIC (8, 'SKU008', 'Product H', 'Category E', 150,47.00);
# MAGIC ---- update the unit price and quantity of one record
# MAGIC UPDATE orders_managed SET unit_price = 100.00,qty=8
# MAGIC  WHERE order_id = 6;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Run a select query to vie your table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_managed

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed

# COMMAND ----------

# MAGIC %md
# MAGIC > We can see 6 versions of data 

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now lets view the data files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from orders_managed

# COMMAND ----------

# MAGIC %md 
# MAGIC > We can see 1 file that points to our data now lets create another insert statement

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_managed VALUES
# MAGIC (9, 'SKU009', 'Product I', 'Category F', 100, 100.00),
# MAGIC (10, 'SKU0010', 'Product J', 'Category G', 50,45.00),
# MAGIC (11, 'SKU0011', 'Product K', 'Category H', 150,47.00);

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets view the file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from orders_managed
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC > We can see there are 2 files that are being used to display the complete data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orders_managed

# COMMAND ----------

# MAGIC %md 
# MAGIC > ### Now We can view the data as well as now lets create a deep clone

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_managed_dclone;
# MAGIC CREATE OR REPLACE TABLE orders_managed_dclone DEEP CLONE orders_managed;
# MAGIC DESCRIBE DETAIL orders_managed_dclone;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets view the files being used in the deep clone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from orders_managed_dclone
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > We can see that it is pointing to the two files that we had in our previous table or the source table we used to clone 
# MAGIC > Now if we dont mention DEEP CLONE and just CLONE in the create statement it will by default create a deep clone.
# MAGIC > Now lets try the scenario where we make changes in the source table that is the managed table used to create the clone

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_managed VALUES
# MAGIC (12, 'SKU0012', 'Product M', 'Category G', 105, 60.00);
# MAGIC
# MAGIC --- View the data 
# MAGIC SELECT * from orders_managed
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the version history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed;

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets view the clone table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from orders_managed_dclone

# COMMAND ----------

# MAGIC %md
# MAGIC We can clearly see their is no record added into the clone table because the clone uses a version 7 of the orders_managed table but after the write operation it has moved to version 8 but the clone is made for version 7 .
# MAGIC
# MAGIC > The conclusion therefore is the source table and clone table are independent of each other after the clone has been made furthermore even if we delete or drop the source table and vacuum the file it will have no impact on the clone table and vice versa because it copies the parquet file form source table along with the meta data which makes it completely independent of the source table.
# MAGIC
# MAGIC > Any changes made to either deep clone or shallow clone effects only themself & not the source table in any case 
# MAGIC
# MAGIC > CTAS statement is not the same as CLONING because in CTAS statement the Constraints are not copied whereas in CLONING all the table properties along with the constraints are replicated as it is in the clone table we can view that below.
# MAGIC
# MAGIC > Also in the case of CTAS a different metadata file and the parquet file is created while in cloning the same file is copied to the cloned table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_copy AS
# MAGIC SELECT * from orders_managed;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC ----- view extended properties
# MAGIC DESCRIBE EXTENDED orders_copy;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from orders_copy;

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now if we check the above properties we dont see out check constraint we apply for the quantity and unit price checking
# MAGIC
# MAGIC > We can also see the file being different if we compare it to the original parquet file and it is newly created and not using the two parquest file for reference as we had seen in cloning.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_managed_dclone;

# COMMAND ----------

# MAGIC %md
# MAGIC HERE WE CAN CLEARLY SEE THE CONSTRAINTS BEING ADDED IN THE TBLPROPERTIES ,
# MAGIC that proves our point that CTAS is not same as CLONING

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMP POINTS TO REMEMBER 
# MAGIC > You can use deep clone to preserve the state of a table at a certain point in the time for archival purpose.
# MAGIC
# MAGIC > To test a workflow on prod table without corrupting a table you can easily create a shallow clone,this allows you to run arbitrary workflows on the cloned table that contains all the prod data but doesnot effect production workloads.
# MAGIC
# MAGIC > Shallow clone on external tables must be external tables , shallow clone on managed table must be managed table.
# MAGIC
# MAGIC > For managed tables,dropping the source table breaks the target table for shallow clones.
# MAGIC
# MAGIC > Shallow clones on external table are not impacted by dropping the source.
# MAGIC
# MAGIC > Create table target_table shallow clone source_table version as of 3;