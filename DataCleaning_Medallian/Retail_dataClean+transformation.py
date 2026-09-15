# Databricks notebook source
# DBTITLE 1,Check Connection
path = "abfss://retail@retailprojectstrdata.dfs.core.windows.net/bronze"

display(dbutils.fs.ls(path))

# COMMAND ----------

# DBTITLE 1,Bronze Layer
df_product = spark.read.parquet("abfss://retail@retailprojectstrdata.dfs.core.windows.net/bronze/product/dbo.products.parquet")
df_transaction = spark.read.parquet("abfss://retail@retailprojectstrdata.dfs.core.windows.net/bronze/transaction/dbo.transactions.parquet")
df_store = spark.read.parquet("abfss://retail@retailprojectstrdata.dfs.core.windows.net/bronze/store/dbo.stores.parquet")

df_customers = spark.read.parquet("abfss://retail@retailprojectstrdata.dfs.core.windows.net/bronze/customer/sagarkh459-sagar/retail-dataPipeline/refs/heads/main/customers.parquet")

display(df_product)
display(df_transaction)
display(df_store)
display(df_customers)

# COMMAND ----------

# DBTITLE 1,Cast Transaction Columns
from pyspark.sql.functions import col

df_transaction = df_transaction.select(
    col("transaction_id").cast("int").alias("transaction_id"),
    col("customer_id").cast("int").alias("customer_id"),
    col("product_id").cast("int").alias("product_id"),
    col("store_id").cast("int").alias("store_id"),
    col("quantity").cast("int").alias("quantity"),
    col("transaction_date").cast("date").alias("transaction_date")
)
display(df_transaction)


df_product = df_product.select(
    col("product_id").cast("int").alias("product_id"),
    col("product_name").cast("string").alias("product_name"),
    col("category").cast("string").alias("category"),
    col("price").cast("decimal(10,2)").alias("price")
)
display(df_product)
from pyspark.sql.functions import col

df_customers = df_customers.select(
    col("customer_id").cast("long").alias("customer_id"),
    col("first_name").cast("string").alias("first_name"),
    col("last_name").cast("string").alias("last_name"),
    col("email").cast("string").alias("email"),
    col("phone").cast("string").alias("phone"),
    col("city").cast("string").alias("city"),
    col("registration_date").cast("date").alias("registration_date")
)
display(df_customers)
df_store = df_store.select(
    col("store_id").cast("int").alias("store_id"),
    col("store_name").cast("string").alias("store_name"),
    col("location").cast("string").alias("location"))

display(df_store)




# COMMAND ----------

# DBTITLE 1,Drop Duplicates
df_customers = df_customers.select("customer_id","first_name","last_name","email","phone","city").dropDuplicates(["customer_id"])

# COMMAND ----------

df_silver = df_transaction.join(df_customers, "customer_id").join(df_product,"product_id").join(df_store,"store_id").withColumn("Total_Sales", col("quantity")*col("price"))
display(df_silver)

# COMMAND ----------

# DBTITLE 1,Dump Silver Dataset in Azure
silver_path = "abfss://retail@retailprojectstrdata.dfs.core.windows.net/silver"

df_silver.write.mode("overwrite").format("delta").save(silver_path)



# COMMAND ----------

# DBTITLE 1,Create silver dataset table
spark.sql("DROP TABLE IF EXISTS silver_dataset")
spark.sql(f"""
CREATE TABLE silver_dataset
USING DELTA
LOCATION "{silver_path}"
""")


# COMMAND ----------

# DBTITLE 1,display silver dataset
display(spark.sql("DESCRIBE DETAIL silver_dataset"))

display(spark.sql("SELECT * FROM silver_dataset"))

# COMMAND ----------

# DBTITLE 1,Gold Layer
silver_df = spark.read.format("delta").load("abfss://retail@retailprojectstrdata.dfs.core.windows.net/silver")

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg , round

gold_df = silver_df.groupBy("transaction_date","product_id","product_name","category","store_id","store_name","location"
                            ).agg(sum("quantity").alias("Total_Quantity_sold"),sum("Total_Sales").alias("Total_sales_amount"), countDistinct("transaction_id").alias("number_of_transactions"),round(avg("total_Sales"),2).alias("Average_transaction_value")
                                  )
display(gold_df)

# COMMAND ----------

gold_path = "abfss://retail@retailprojectstrdata.dfs.core.windows.net/gold"

gold_df.write.mode("overwrite").format("delta").save(gold_path)

# COMMAND ----------

spark.sql("DROP Table IF EXISTS gold_dataset")

spark.sql(f"""
          CREATE TABLE gold_dataset
          USING DELTA
          LOCATION "{gold_path}"
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold_dataset

# COMMAND ----------

