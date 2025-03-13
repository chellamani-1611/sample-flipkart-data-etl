# Databricks notebook source
# DBTITLE 1,Reading data
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/Flipkart_sample_1000.csv")




# COMMAND ----------

# DBTITLE 1,dropping columns that dont need for analysis
df_cleaned = df.drop("uniq_id", 
                     "crawl_timestamp", 
                     "product_url", 
                     "image", 
                     "description", 
                     "product_specifications", 
                     "is_FK_Advantage_product")



# COMMAND ----------

# DBTITLE 1,category
from pyspark.sql.functions import split, regexp_replace, when, col, size


df_cleaned = df.withColumn(
    "cleaned_category_tree",
    regexp_replace(col("product_category_tree"), r'["\[\]]', '')  # Remove quotes and square brackets
)


df_cleaned = df_cleaned.withColumn(
    "category", split(col("cleaned_category_tree"), " >> ").getItem(0)
)


df_cleaned = df_cleaned.withColumn(
    "category",
    when(size(split(col("category"), " ")) > 5, "Others").otherwise(col("category"))
)


df_cleaned = df_cleaned.drop("cleaned_category_tree")

df_transformed = df_transformed.drop("product_category_tree")




# COMMAND ----------

# DBTITLE 1,converting datatypes
from pyspark.sql.functions import col


df_converted = df_cleaned.withColumn("retail_price", col("retail_price").cast("double")) \
                         .withColumn("discounted_price", col("discounted_price").cast("double")) \
                         .withColumn("product_rating", col("product_rating").cast("double")) \
                         .withColumn("overall_rating", col("overall_rating").cast("double"))




# COMMAND ----------

# DBTITLE 1,dropin unwanted from transformed
columns_to_drop = ["uniq_id", "crawl_timestamp", "product_url", "image", 
                   "is_FK_Advantage_product", "description", "product_specifications", "brand"]

df_transformed = df_converted.drop(*columns_to_drop)



# COMMAND ----------

# DBTITLE 1,missing rating
df_transformed = df_transformed.fillna({
    "retail_price": 0.0, 
    "discounted_price": 0.0, 
    "product_rating": 0.0,   
    "overall_rating": 0.0
})


