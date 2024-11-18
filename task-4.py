from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

users_df = spark.read.csv('./test.csv/users.csv', header=True)
purchases_df = spark.read.csv('./test.csv/purchases.csv', header=True)
products_df = spark.read.csv('./test.csv/products.csv', header=True)

merged_df = purchases_df.join(
    users_df,
    purchases_df["user_id"] == users_df["user_id"],
    how="inner"
).join(
    products_df,
    purchases_df["product_id"] == products_df["product_id"],
    how="inner"
)

filtered_df = merged_df.filter((col("age") >= 18) & (col("age") <= 25))

category_total_df = filtered_df.groupBy("category").agg(
    spark_sum(col("price") * col("quantity")).alias("total_sales")
)

print("Total sales per category (Age 18-25):")
category_total_df.show()

spark.stop()
