# Databricks notebook source
# MAGIC %md
# MAGIC ## 10-12-2025

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SCHEMA EVOLUTION AND ENFORCEMENT

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Use the same concept of creating a volume and directory and adding the data 

# COMMAND ----------

# MAGIC %sql
# MAGIC Create volume if not exists schema_evolution_and_enforcement_demo

# COMMAND ----------

# MAGIC %md
# MAGIC Create orders_managed directory

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed')

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Create a dataframe and then add it to the volume

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
volume_path='/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed'
df.write.format('delta').mode('overwrite').save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCHEMA ENFORCEMENT - 
# MAGIC is the process of strictly maintaining a predefined schema rejecting any data that doesnot confirm to it to ensure data quality and consistency.
# MAGIC #### Rejecting any data that doesnot confirm to it

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md 
# MAGIC If we see the results above it gives us the data type of each column that currently the schema is following the enforcement means that any data that we are going to add to this schema or table should be of the same data type that is of the column currently if it is not the insert fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lets demonstrate it with an example 
# MAGIC We will create a temp view that will have quantity in words rather than in INT.
# MAGIC
# MAGIC ##### NOTE - Databricks also does implicit type conversion if it is able to typecast it internally it wont give error it and convert to required format for example if we give '2' it will convert it to int 2 we will see it in another example.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bad_data AS
# MAGIC SELECT * FROM VALUES 
# MAGIC (6,'sku-5','Bad Row','Mis','two',99.00) AS t(product_id,product_sku,product_name,product_category,product_qty,product_price)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed` AS t
# MAGIC USING bad_data AS s
# MAGIC ON t.product_id = s.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can see the error STRING cannot be converted to int 
# MAGIC Now Lets see the option we discussed earlier

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bad_data AS
# MAGIC SELECT * FROM VALUES 
# MAGIC (6,'sku-5','Bad Row','Mis','2',99.00) AS t(product_id,product_sku,product_name,product_category,product_qty,product_price)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed` AS t
# MAGIC USING bad_data AS s
# MAGIC ON t.product_id = s.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### If you see above the insert operation gave success because databricks was internally able to convert '2' into the int 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can find our record written in the delta table 
# MAGIC Now lets view the table description

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC     

# COMMAND ----------

# MAGIC %md 
# MAGIC We can see the data type is still the same 'int' for product_qty and it typecasted '2' to int 2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now lets view the history of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC ### ADD ANOTHER COLUMN INTO DATA 

# COMMAND ----------

# MAGIC %sql
# MAGIC ----- added another column discount with 5.0 value
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bad_data AS
# MAGIC SELECT * FROM VALUES 
# MAGIC (6,'sku-5','Bad Row','Mis','2',99.00,5.0) AS t(product_id,product_sku,product_name,product_category,product_qty,product_price,discount)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### LETS TRY TO MERGE THIS DATA INTO OUR DELTA TABLE

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed` AS t
# MAGIC USING bad_data AS s
# MAGIC ON t.product_id = s.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now if we see the results databricks didnot add the discount column to our existing table rather it updated the columns that were present in our schema and ignored the column that was not present in our original schema.
# MAGIC
# MAGIC #### NOTE- This happened because we are using serverless mode , if we use our cluster then we have to enable the property .option('mergeSchma','true')

# COMMAND ----------

# MAGIC %md 
# MAGIC Lets add a new column discount in our original column 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC ADD COLUMN discount DOUBLE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md 
# MAGIC View the data now

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC We will have null as the value of our discount column since we havent added the data , now lets add the data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO DELTA.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC SELECT * FROM bad_data;

# COMMAND ----------

# MAGIC %md
# MAGIC View the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md 
# MAGIC > **This was all about the SCHEMA ENFORCEMENT concept.**

# COMMAND ----------

# MAGIC %md 
# MAGIC > **SCHEMA EVOLUTION**

# COMMAND ----------

# MAGIC %md
# MAGIC Lets add another column to our temporary view with promo code

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW order_data AS
# MAGIC SELECT * FROM VALUES
# MAGIC (8,'sku-6','product-6','category-6',10,100.00,0.0,90.0) AS t(product_id,product_sku,product_name,product_category,product_qty,product_price,discount,promocode)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WHAT TO USE
# MAGIC > Theory -
# MAGIC * In Normal scenario if we use MERGE INTO it will write the data for which the schema is present & ignore the column not present
# MAGIC * So in real world scenario we wont want to loose any data that we are receiving from the source so thats where SCHEMA EVOLUTION comes into picture.
# MAGIC * We can achive this by adding keyword MERGE WITH SCHEMA EVOLUTION .

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- using the above logic
# MAGIC MERGE WITH SCHEMA EVOLUTION INTO delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed` AS target
# MAGIC USING order_data AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC Lets view the updated data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC ----- we can see the schema being updated

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC >  In the hostory we can view the various operations performed and if we view the latest version which shows the MERGE OPERATION

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPARK WAY

# COMMAND ----------

# MAGIC %md #### We will create another spark dataframe and then try to merge it to the existing table we have

# COMMAND ----------

# MAGIC %md
# MAGIC Create the spark dataframe and then write it to the volume but to add the option of Schema Evolution we have to add the option **mergeSchema - True**

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, LongType,StringType,IntegerType,DecimalType
data=[
    (5001,'sku-101','yoga','fitness',3,'PROMO20'),
    (5002,'sku-102','yoga','fitness',2,'PROMO30')
    
]
schema=StructType([
    StructField('product_id',LongType(),False),
    StructField('product_sku',StringType(),True),
    StructField('product_name',StringType(),True),
    StructField('product_category',StringType(),True),
    StructField('product_qty',IntegerType(),True),
    StructField('promo',StringType(),True)
])
df=spark.createDataFrame(data,schema=schema)
display(df)
df.printSchema()
df.write.format('delta').mode('overwrite').option('mergeSchema','true').save('/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed')

# COMMAND ----------

# MAGIC %md 
# MAGIC After adding the above option we also want to make sure that we have the capability to have type widening

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC SET TBLPROPERTIES (
# MAGIC   delta.enableTypeWidening = true
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md 
# MAGIC We can see that our property for type widening is enabled successfully...

# COMMAND ----------

# MAGIC %md
# MAGIC > THEORY
# MAGIC * Now the above scenario will be used to show how to change the data type of the column according to the column data of the source .
# MAGIC * Lets say in the source we change the data type of qty from int to long 
# MAGIC * What will happen if we dont set the property is it will will give an error during mergeSchema 
# MAGIC * SO we will enable the table property 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### AS WE CAN SEE ABOVE 
# MAGIC I made an error so I will go back to my previous version to fix this 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Restoring my Table to version 5 as it had all the data

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`
# MAGIC TO VERSION AS OF 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now will add the dataframe again with correct values 

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, LongType,StringType,IntegerType,DecimalType
data=[
    (5001,'sku-101','yoga','fitness',3,'PROMO20'),
    (5002,'sku-102','yoga','fitness',2,'PROMO30')
    
]
schema=StructType([
    StructField('product_id',LongType(),False),
    StructField('product_sku',StringType(),True),
    StructField('product_name',StringType(),True),
    StructField('product_category',StringType(),True),
    StructField('product_qty',IntegerType(),True),
    StructField('promocode',StringType(),True)
])
df=spark.createDataFrame(data,schema=schema)
display(df)
df.printSchema()
df.write.format('delta').mode('append').option('mergeSchema','true').save('/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now we can see the error that is cause due to type widening 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### IF WE SEE THE TYPE WIDENING DIDNOT WORK SO WE WILL HAVE TO EXPLICITLY CONVERT DATA TYPE TO THE DECIMAL DATA TYPE 

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, LongType,StringType,IntegerType,DoubleType,DecimalType
data=[
    (5001,'sku-101','yoga','fitness',3,Decimal(45.00)),
    (5002,'sku-102','yoga','fitness',2,Decimal(52.00))
    
]
schema=StructType([
    StructField('product_id',LongType(),False),
    StructField('product_sku',StringType(),True),
    StructField('product_name',StringType(),True),
    StructField('product_category',StringType(),True),
    StructField('product_qty',IntegerType(),True),
    StructField('promocode',DecimalType(10,2),True)
])
df=spark.createDataFrame(data,schema=schema)
display(df)
df.printSchema()
df.write.format('delta').mode('append').option('mergeSchema','true').save('/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/orders_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC > SCHEMA EVOLUTION
# MAGIC is the flexible process of updating and adapting a dataset's schema to accomodate changes such as adding new columns or modifying data types ,without breaking existing data pipelines.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### RENAME A COLUMN OR DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC #### FOR THIS USE CASE WE WILL CREATE A NEW VOLUME/DIRECTORY AND TEST IT THERE 

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case')

# COMMAND ----------

# MAGIC %md 
# MAGIC Lets use the same dataframe and add it 

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, LongType,StringType,IntegerType,DoubleType,DecimalType
data=[
    (5001,'sku-101','yoga','fitness',3,Decimal(45.00)),
    (5002,'sku-102','yoga','fitness',2,Decimal(52.00)),
    (5003,'sku-103','yoga','fitness',4,Decimal(525.00)),
    (5004,'sku-104','yoga','fitness',25,Decimal(582.00)),
    
]
schema=StructType([
    StructField('product_id',LongType(),False),
    StructField('product_sku',StringType(),True),
    StructField('product_name',StringType(),True),
    StructField('product_category',StringType(),True),
    StructField('product_qty',IntegerType(),True),
    StructField('promocode',DecimalType(10,2),True)
])
df=spark.createDataFrame(data,schema=schema)
display(df)
df.printSchema()
df.write.format('delta').mode('overwrite').option('mergeSchema','true').save('/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case')

# COMMAND ----------

# MAGIC %md 
# MAGIC DO A SELECT * AND VIEW THE HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case`

# COMMAND ----------

# MAGIC %md
# MAGIC #### LETS TRY TRADITIONAL WAY TO RENAME A COLUMNS

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE DELTA.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case`
# MAGIC RENAME COLUMN product_qty TO product_quantity 

# COMMAND ----------

# MAGIC %md
# MAGIC When we run the above command we see the error because column rename is not supported for delta tables we need to enable ('delta.columnMapping.mode' = 'name') Lets enable it
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case`
# MAGIC SET TBLPROPERTIES('delta.columnMapping.mode' = 'name')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now we will see that there are 2 jsons and 1 parquet file created , lets run the rname command now

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE DELTA.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case`
# MAGIC RENAME COLUMN product_qty TO product_quantity 

# COMMAND ----------

# MAGIC %md
# MAGIC > THEORY(IMPORTANT)-
# MAGIC * There will only be 1 parquet file but there will be 2 log files or metadata files that will be created.
# MAGIC * There is no change in the data file the whole magic happens at the transactional logs or the meta data file.
# MAGIC * Now if we view the change using DESCRIBE FORMATTED we will find the change is done logically rather than physically.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case`

# COMMAND ----------

# MAGIC %md
# MAGIC If you check the Column Names in the Detailed Statistics the column name still shows product_qty since it didnot change it at the physical locatrion but only changed it logically that we can view when we run the select query or the col_name at the start
# MAGIC * It doesnot have to rewrite the parquet file.
# MAGIC * It will just add a json file to use it logically.

# COMMAND ----------

# MAGIC %md
# MAGIC #### DROP COLUMN
# MAGIC The same logic happens in dropping the column as well

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case`
# MAGIC DROP COLUMN promocode

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets see what exactly happened

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED delta.`/Volumes/workspace/default/schema_evolution_and_enforcement_demo/rename_use_case`

# COMMAND ----------

# MAGIC %md
# MAGIC > DETAILS
# MAGIC Now we can see the same thing it hasnt dropped the column physically that we can view the Delta statistics column but it dropped it logically by creating a json file.
# MAGIC * You can see 3 json files one for create one for rename and one for drop but there is only 1 parquet file.

# COMMAND ----------

