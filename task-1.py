from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

users_df = spark.read.csv('./test.csv/users.csv', header=True)
purchases_df = spark.read.csv('./test.csv/purchases.csv', header=True)
products_df = spark.read.csv('./test.csv/products.csv', header=True)

users_df.createTempView("users_view")
purchases_df.createTempView("purchases_view")
products_df.createTempView("products_view")

users_df.show(10)
purchases_df.show(10)
products_df.show(10)

spark.stop()