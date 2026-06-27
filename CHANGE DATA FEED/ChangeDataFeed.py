# Databricks notebook source
# MAGIC %md
# MAGIC ## 12 DEC 2025
# MAGIC ### CHANGE DATA FEED

# COMMAND ----------

# MAGIC %md
# MAGIC > Change Data Feed -
# MAGIC is used to track the row level changes between different versions of a table.
# MAGIC
# MAGIC > Example-
# MAGIC * We are going from v0 -v2 
# MAGIC * Have multiple
# MAGIC   * inserts
# MAGIC   * deletes
# MAGIC   * updates
# MAGIC * It will store the pre-image and post-image after these operations
# MAGIC * like before update lets say order_id =2 that will be the pre image and post update it is 5 that will be the post image, using CDF we will be able to view both the values which helps us in maintaing data efficeiently and we can view what update or changes are made in the value because of an operation performed.
# MAGIC
# MAGIC > Incremental Data Loading - One of the most beneficial advantage of CDF is it processes only changed data that is referred to as incremental load in programmatical terms.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Now Lets Understand this using an example first using SQL and then PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC #### We will follow the same process of creating a volume and then use it as a delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Volume if not exists change_data_feed

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/change_data_feed/orders')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insert the Records using dataframe

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
volume_path='/Volumes/workspace/default/change_data_feed/orders'
df.write.format('delta').mode('overwrite').save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC > View the History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/change_data_feed/orders`

# COMMAND ----------

# MAGIC %md 
# MAGIC > The change data feed is not automatically enabled in the Table we need to add it in the table properties to use it so lets add t now

# COMMAND ----------

# MAGIC %sql
# MAGIC --- using alter table to update out delta table to enable the change data feed
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/change_data_feed/orders`
# MAGIC SET TBLPROPERTIES(delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets view the history to verify if CDF is enabled or not

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY  delta.`/Volumes/workspace/default/change_data_feed/orders`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC We can see that a new log is created for Change data feed where it is set to be true

# COMMAND ----------

# MAGIC %md
# MAGIC > IMP NOTE- We can only view the Change Data Feed after it is enabled on the table, Now if we see the Change Data Feed records for the version 0 we will not find anything and will get an error for [DELTA_MISSING_CHANGE_DATA]
# MAGIC > The function or the query we use to get this information is [Select * from table_changes('table','version start from','version to ')]
# MAGIC Now lets try and run this command for version v0 to check the error mentioned above

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('delta.`/Volumes/workspace/default/change_data_feed/orders`',0)

# COMMAND ----------

# MAGIC %md 
# MAGIC > If you see above the same error is being mentioned that we talked about and says to us check if change data feed is enabled for the version above or not 

# COMMAND ----------

# MAGIC %md
# MAGIC ### For Testing and viewing this record we would have to make an update that will create a new version for us

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/Volumes/workspace/default/change_data_feed/orders` SET product_price = 1000 WHERE product_id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets view the history of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/change_data_feed/orders`
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now here we can see a new version 2 which has done update operation and since we added change data feed after version 1 we will be able to see the change data feed .
# MAGIC
# MAGIC Lets view the changes

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from table_changes('delta.`/Volumes/workspace/default/change_data_feed/orders`',2)

# COMMAND ----------

# MAGIC %md 
# MAGIC - > If you view the result you can find that there is a column _change_type which gives us the update_pre and post image and we can see the update_price so this helps us in understanding the changes made to our table.
# MAGIC - > But solely viewing this doesnot solve our purpose since we can only view the upstream delta table i.e the orders delta table the practical use case would be to capture the changes over a streaming table so that we can view it in real time whenever a change is made in the upstream table.
# MAGIC - > For that use case lets create a delete operation on the upstream table and then store those details in our streaming table

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/Volumes/workspace/default/change_data_feed/orders` WHERE product_id = 1
# MAGIC     

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets view the history 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/change_data_feed/orders`

# COMMAND ----------

# MAGIC %md
# MAGIC > We can view a new version 4 being created for the delete operation 

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets view the changes in feed over the table_changes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('delta.`/Volumes/workspace/default/change_data_feed/orders`',1)

# COMMAND ----------

# MAGIC %md
# MAGIC - > If we see the results above we can see that the records show the update and delete change feed .
# MAGIC - > If we think carefully it will be more practical to keep track of the deleted records so that we are update with the delete operations made since the data is most critical for us.
# MAGIC - > Lets create the streaming table for delete operations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE product_stream AS
# MAGIC SELECT * from table_changes('delta.`/Volumes/workspace/default/change_data_feed/orders`',3)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM product_stream

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we see my mistake we cannot create a streaming table directly rather it should be populated using the pipeline so we will create a normal managed delta table for our case 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE products_deleted AS 
# MAGIC SELECT * FROM table_changes('delta.`/Volumes/workspace/default/change_data_feed/orders`',3) WHERE _change_type = 'delete'
# MAGIC     
# MAGIC      

# COMMAND ----------

# MAGIC %md
# MAGIC > LETS VIEW THE DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from products_deleted

# COMMAND ----------

# MAGIC %md
# MAGIC **We can clearly view the records over the table but it still doesnot give real time changes data because we need to create table again and again even if we remove more records**

# COMMAND ----------

# MAGIC %md 
# MAGIC > Lets delete one more record from the original delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/Volumes/workspace/default/change_data_feed/orders` WHERE product_id = 3

# COMMAND ----------

# MAGIC %md 
# MAGIC > LETS VIEW THE CTABLE CHANGES MADE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('delta.`/Volumes/workspace/default/change_data_feed/orders`',3)

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now lets see the records in our product_delete table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from products_deleted

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we see that in our product_deleted table only 1 record is showing even though we have deleted two records this is the problem with normal managed tables hence we need to create a streaming table that catches real time records or changes happening over the table thats where the pyspark straming table helps us.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## LETS CREATE THE SPARK TABLE BUT BEFORE THAT LETS GO BACK TO OUR ORIGINAL TABLE 

# COMMAND ----------

# MAGIC %md
# MAGIC RESTORE COMMAND TO GO BACK TO VERSION 0

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE delta.`/Volumes/workspace/default/change_data_feed/orders` TO VERSION AS OF 0;
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/change_data_feed/orders`;

# COMMAND ----------

# MAGIC %md
# MAGIC We have gone back to our original table lets view table properties

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/workspace/default/change_data_feed/orders`

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now if we see when we restore to the version 0 we dont have change data feed enabled.
# MAGIC > For this test case we will create a managed table using our existing volume delta table 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_managed_cdf ;
# MAGIC CREATE OR REPLACE TABLE orders_managed_cdf AS
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/change_data_feed/orders`;
# MAGIC
# MAGIC
# MAGIC
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now lets see history for our newly created table 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed_cdf

# COMMAND ----------

# MAGIC %md 
# MAGIC > We clearly see a version 0, now lets add the alter table property to enable change data feed and then delete 1 record

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_managed_cdf 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true) ;
# MAGIC DELETE FROM orders_managed_cdf WHERE product_id = 3;

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets view the history and table_changes

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed_cdf;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM table_changes('orders_managed_cdf',2)

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets create the streaming table with the help of pyspark

# COMMAND ----------

from pyspark.sql.functions import col
(
    spark.readStream.format('delta')
    .option('readChangeFeed','true')
    .option('startingVersion','2')
    .table('orders_managed_cdf')
    .filter(col('_change_type')=='delete')
    .select('*')
    .writeStream
    .outputMode('append')
    .option('checkpointLocation','/Volumes/workspace/default/change_data_feed/checkpoints/')
    .trigger(availableNow=True)
    .table('orders_deleted_stream')
)

# COMMAND ----------

# MAGIC %md 
# MAGIC > IMPORTANT POINTS IN ABOVE SCRIPT
# MAGIC - readChangeFeed ---equivalent to table_changes in sql 
# MAGIC - we mentioned table instead of that we could have also used volume using load
# MAGIC - filter command --- filters the results in our case it will have only the deleted records
# MAGIC - output mode --- gives us how we want to add the data to a streaming table
# MAGIC - checkPoints --- help us in going to a particular operations that is it helps us maintain the incremental data loads so after it has performed any operation it will not visit it again that is going to save time for example if it has done 1 delete operation it will not delete that delete operation if we rerun the stream for another delete operation it does it by saving what has happened in checkpoints and it is mandatory to provide.
# MAGIC - triggers --- define how the streaming table will be called we have set it to availableNow=True to run it whenever we call the streaming table if we use other operations it will automatically run the select statement over the streaming table for eg- processingTime=2 seconds will trigger the script every 2 seconds by default if we dont define anything it automatically runs it every half a second or 500 mili seconds, we cannot use processingTime in serverless because it is not supported.
# MAGIC - table ---- defines the name of our streaming table

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_managed_cdf WHERE product_id = 2;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_deleted_stream

# COMMAND ----------

# MAGIC %md
# MAGIC - > Now to use this use the stream what we need to do is whenever we perform any delete operation on our original table(orders_managed_cdf) like in cell 65 we will have to rerun the stream again at cell 63 then we can view the results in the streaming table records in cell 66 .
# MAGIC - > This is not the perfect use case since we have to run the stream manually becaue we have trigger set as availableNow = True but in production environment we will use trigger as processingTime that is going to be a huge advantage in maintaining the pipeline as whenever the delete operation is performed we can view the data straightaway in real time over the streaming table.
# MAGIC - > Now once more important thing to remember is the streaming statement is idempotent that is it will not make any changes or doesnot do anything if there is no delete operation made on the original table.This is how it is different from INSERT statement as if you run the INSERT statement multiple times it will keep opn adding redundant data in the table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ENABLE CDF AT TABLE CREATION LEVEL
# MAGIC > We can create the change data feed option or enable it while we create a table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )USING DELTA
# MAGIC TBLPROPERTIES(delta.enableChangeDataFeed=true);
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now if you view the Table Properties you will find it enabled 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES demo_table
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ENABLE CDF AT SESSION LEVEL
# MAGIC > We can also enable CDF at Session Level that will auto enable the CDF on every table we create over the session 

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = TRUE
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC > SINCE WE ARE USING FREE VERSION THIS OPTION IS NOT AVAILABLE FOR US