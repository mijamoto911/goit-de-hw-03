from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()


purchases_df = spark.read.csv('./test.csv/purchases.csv', header=True)
products_df = spark.read.csv('./test.csv/products.csv', header=True)

merged_df = purchases_df.join(
    products_df,
    purchases_df["product_id"] == products_df["product_id"],
    how='inner'
)

category_total_df = merged_df.groupBy("category").agg(
    spark_sum(col("price") * col("quantity")).alias("total_sales")
)

print("Total sales per category:")
category_total_df.show()

spark.stop()
