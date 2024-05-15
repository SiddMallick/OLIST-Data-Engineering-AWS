from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomerIngestion").getOrCreate()

customer_df = spark.read.parquet('D:/Data_Engineering_Projects/OLIST-Data-Engineering-AWS/processed/customers.parquet')

customer_df.show()
