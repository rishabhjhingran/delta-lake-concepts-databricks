# Databricks notebook source
# MAGIC %md
# MAGIC # 12 DEC 2025
# MAGIC ## CLONING -- SHALLOW CLONING

# COMMAND ----------

# MAGIC %md
# MAGIC > CLONING
# MAGIC - Create a copy of an existing delta table.
# MAGIC > CLONING IS OF TWO TYPES-
# MAGIC -     SHALLOW CLONE - copies only the source table metadata to your target table.
# MAGIC -     DEEP CLONE - copies the meta data and data from source to your target table.

# COMMAND ----------

# MAGIC %md 
# MAGIC > ## SHALLOW CLONING 
# MAGIC - Use the same concept to create a volume directory and then add the data

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Volume if not exists cloning_demo

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/cloning_demo/products')

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, LongType,StringType,IntegerType,DecimalType
data=[
    (1,'sku-1001','yoga','fitness',3,Decimal("109.00")),
    (2,'sku-2001','dumbell','fitness',5,Decimal("509.00")),
    (3,'sku-3001','mat','fitness',1,Decimal("809.00")),
    (4,'sku-4001','back massager','fitness',7,Decimal("459.00")),
    (5,'sku-5001','protein bar','fitness',4,Decimal("809.00"))
]
schema=StructType([
    StructField('product_id',LongType(),False),
    StructField('product_sku',StringType(),True),
    StructField('product_name',StringType(),True),
    StructField('product_category',StringType(),True),
    StructField('product_qty',IntegerType(),True),
    StructField('product_price',DecimalType(10,2),True)
])
df=spark.createDataFrame(data,schema=schema)
display(df)
df.printSchema()
volume_path='/Volumes/workspace/default/cloning_demo/products'
df.write.format('delta').mode('overwrite').save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC > This Demo is mostly critical to view from the catalog point of view , right now if you see the volume you would find 1 parquet file and 1 log file .

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/cloning_demo/products`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/cloning_demo/products`

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now we will visit the concept of viewing the metadata multiple times to find which file is creating the Select query results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path FROM delta.`/Volumes/workspace/default/cloning_demo/products`
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets add a constraint 

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/cloning_demo/products`
# MAGIC ADD CONSTRAINT check_qty CHECK(product_qty > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC > VIEW THE DATA HISTORY 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/cloning_demo/products`

# COMMAND ----------

# MAGIC %md 
# MAGIC > We can view 2 versions of data now lets view how many parquet files we have

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path FROM delta.`/Volumes/workspace/default/cloning_demo/products`

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets create an update and add 3 more rows  as well

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/Volumes/workspace/default/cloning_demo/products`
# MAGIC SET product_price =1000
# MAGIC where product_id = 1;
# MAGIC
# MAGIC INSERT INTO delta.`/Volumes/workspace/default/cloning_demo/products`
# MAGIC VALUES (1000,'sku1000','product1000','category1000',1000,1000),
# MAGIC (1001,'sku1001','product1001','category1001',1001,1001),
# MAGIC (1002,'sku1002','product1002','category1002',1002,1002);

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets view the data history and find the curent files that we are going to use

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/cloning_demo/products`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from delta.`/Volumes/workspace/default/cloning_demo/products`

# COMMAND ----------

# MAGIC %md
# MAGIC > If we see we have 4 versions of our data the last version(3) is for optimize and the second last operation(2) is write and if we view our _metadata to find the parquet files that are being used we find two files that are -
# MAGIC - Optimized parquet file
# MAGIC - Write parwquet file
# MAGIC Transactional Logs will use these 2 files to view our data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### LETS CREATE A SHALLOW CLONE 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > We wont be able to view the delta logs and files since we need to create a managed table so we will use history and _metadata.file_path multiple times to view the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS product_clone ;
# MAGIC CREATE OR REPLACE TABLE product_clone SHALLOW CLONE delta.`/Volumes/workspace/default/cloning_demo/products`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if you check we cant create a clone over the volume delta table so we will first create a managed table product and then use this 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE products AS
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/cloning_demo/products` ;
# MAGIC
# MAGIC SHOW TBLPROPERTIES products;

# COMMAND ----------

# MAGIC %md
# MAGIC > Now since our managed table is created lets try to replicate the same operations we did before to this managed table so that we can deduce the correct logic.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from products

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY products

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets do the 3 inserts and update operations

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE products
# MAGIC SET product_price =2500
# MAGIC where product_id = 1;
# MAGIC
# MAGIC INSERT INTO products
# MAGIC VALUES (2000,'sku2000','product2000','category2000',2000,1000),
# MAGIC (2001,'sku2001','product2001','category2001',2001,1001),
# MAGIC (2002,'sku2002','product2002','category2002',2002,1002);

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the history and details to find the files currenlty pointing to our data

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY products;

# COMMAND ----------

# MAGIC %md
# MAGIC > We can see we have 7 versions of our table , 6th version is optiomize Now lets view the files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from products

# COMMAND ----------

# MAGIC %md
# MAGIC > We can  see that we are using two parquet files that are representing our current data 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### LETS TRY CLONING AGAIN

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_clone SHALLOW CLONE products;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if you check column source_num_of_files we can see that it is using the last 2 parquet files that we had for our original table but it is actually not going to have that file it will just have the reference to those files in its transactional log that will be created during cloning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL product_clone
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY product_clone

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if you check the operationMetrics above you will find there is no file added or removed or copied but it shows sourceNumOfFiles 2 which shows that it is referencing to the two parquet files from the source table

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets add two more records to the clone table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO product_clone
# MAGIC VALUES (3000,'sku3000','product3000','category3000',3000,1000),
# MAGIC (3001,'sku3001','product3001','category3001',3001,1001),
# MAGIC (3002,'sku3002','product3002','category3002',3002,1002);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from product_clone;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > If you see it has all the records from the product table and the three records we inserted right now

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets view the files currenlty being used for our clone table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from product_clone

# COMMAND ----------

# MAGIC %md
# MAGIC > If you see closely it uses 2 parquet file from our source table that is product and 1 parquest that we have created by using the insert operation

# COMMAND ----------

# MAGIC %md 
# MAGIC #### NOW LETS ADD THREE RECORDS FOR THE MAIN SOURCE PRODUCT TABLE TO CHECK IF IT IS UPDATED IN OUR CLONE TABLE BUT FIRST LETS CHECK IF THE INSERT OPERATIO WE DID ON CLONE TABLE MADE ANY CHANGE ON OUR MAIN TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from products

# COMMAND ----------

# MAGIC %md
# MAGIC > We can see no change is made on the original table w=if we make change to the clone table now lets try the reverse of it and make changes on main table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO products
# MAGIC VALUES (4000,'sku3000','product3000','category3000',3000,1000),
# MAGIC (4001,'sku3001','product3001','category3001',3001,1001),
# MAGIC (4002,'sku3002','product3002','category3002',3002,1002);
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets View the History of our main table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY products

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the clone table to see if their is any change

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM product_clone

# COMMAND ----------

# MAGIC %md
# MAGIC > We dont see any change in the clone table because it is the clone of a particular version of the product table to be precise it is the clone of version 6 so the current version of source table product is 7 so there will be no change made on the clone table even if we make any change on the source table.

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets try to delete some records from clone table and then view which file it is going to use

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from product_clone where product_id=2001

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from product_clone

# COMMAND ----------

# MAGIC %md
# MAGIC > NOW WE CAN SEE THAT WE SRE POINITNG TO ONLY ONE PARQUET FILE

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SCENARIOS -
# MAGIC #### DELETE EVERYTHING FROM BASE TABEL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM products

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets run select query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * from products

# COMMAND ----------

# MAGIC %md 
# MAGIC > We can see there is no record but there will still be files so lets Vacuum and delete all the files as well for that we need to alter table properties 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE products
# MAGIC SET TBLPROPERTIES('delta.deletedFileRetentionDuration'= 'interval 0 hours')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES products

# COMMAND ----------

# MAGIC %md 
# MAGIC > Always best practice to do DRY RUN first and then vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM products DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC > We could view the files not being deleted because that are being used in our clone

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM products;

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we run the records for id that was being used for product_id =3 that was being cloned from the products tabkle that we have removed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from product_clone WHERE product_id=3

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see that even though the data files are deleted from products the files that were being used for cloning activity were not deleted and were kept so that clone doesnot break thatis the advantage of dtabricks.
# MAGIC
# MAGIC > Now lets try to bring the source data to the version we used to clone 

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE products TO VERSION AS OF 6

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets view the files being used for the above version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from products

# COMMAND ----------

# MAGIC %md
# MAGIC > We can clearly see the two files used for this particular version and for cloning

# COMMAND ----------

# MAGIC %md
# MAGIC > NOTE:- You may be able to view the file for a different version because it may be in the cache

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_path from products@v7

# COMMAND ----------

# MAGIC %md 
# MAGIC > You can clearly see the file beiung removed gives error when trying to view the version 

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets have a scenario where we delete the clone table and then vacuum the files and see what happens to the source product table

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from product_clone

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets set the table property and perform a dry run 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE product_clone
# MAGIC SET TBLPROPERTIES('delta.deletedFileRetentionDuration'= 'interval 0 hours');
# MAGIC VACUUM product_clone DRY RUN;
# MAGIC VACUUM product_clone;
# MAGIC     
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets view the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_path from product_clone

# COMMAND ----------

# MAGIC %md
# MAGIC > Now we will check in product 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC %md
# MAGIC > THEORY -
# MAGIC - > If the source table is referrencing to something we cannot use clone table to remove them.
# MAGIC - > Similarly if target table is referencing to some files of source we cannot delete them using target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY products

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets see what happens when we DROP table

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets drop the source table products

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE products

# COMMAND ----------

# MAGIC %md
# MAGIC > ## THEORY
# MAGIC - it will drop it but will keep the files for 7 days & we will have the option to undrop it 
# MAGIC - after 7 days the garbage collector will remove everything
# MAGIC
# MAGIC > The clone table will be able to use the files for 7 days 
# MAGIC - We can use clone to refer those files for 7 days.