from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

users_df = spark.read.csv('./test.csv/users.csv', header=True)
purchases_df = spark.read.csv('./test.csv/purchases.csv', header=True)
products_df = spark.read.csv('./test.csv/products.csv', header=True)

# Видалення рядків з пропущеними значеннями
cleaned_users_df = users_df.dropna()
cleaned_purchases_df = purchases_df.dropna()
cleaned_products_df = products_df.dropna()

print("Cleaned Users DataFrame:")
cleaned_users_df.show(10)
print("Cleaned Purchases DataFrame:")
cleaned_purchases_df.show(10)
print("Cleaned Products DataFrame:")
cleaned_products_df.show(10)

spark.stop()