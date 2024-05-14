import findspark
findspark.init('/home/ubuntu/spark-3.5.1-bin-hadoop3')
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# AWS credentials
aws_access_key_id = input('access_key')
aws_secret_access_key = input('secret_key')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.720,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell'

conf = SparkConf().setAppName('S3toSpark')

sc = SparkContext(conf = conf)

spark = SparkSession(sc).builder.appName('S3App').getOrCreate()

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id )
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')


customer_schema = StructType([StructField('customer_id', StringType(), True),\
                              StructField('customer_unique_id', StringType(), True),\
                             StructField('customer_zip_code_prefix', StringType(), True),\
                              StructField('customer_city', StringType(), True),
                              StructField('customer_state', StringType(),True)])

read_path = 's3a://olist-data-lake/raw'
write_path = 's3a://olist-data-lake/processed'

customer_df = spark.read.schema(customer_schema).option('header',True).csv(f'{read_path}/customers.csv')
customer_df.show()
customer_df.write.mode('overwrite').parquet(f'{write_path}/customers')

order_items_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('order_item_id', IntegerType(), True),
    StructField('product_id', StringType(), True),
    StructField('seller_id', StringType(), True),
    StructField('shipping_limit_date', DateType(), True),
    StructField('price', FloatType(), True),
    StructField('freight_value', FloatType(), True)
])

order_items = spark.read.schema(order_items_schema).option('header',True).csv(f'{read_path}/order_items.csv')

order_items_final = order_items.withColumn('total_price', col('price')*col('order_item_id') + col('freight_value')*col('order_item_id'))

order_items_final.show(5)

order_items_final.write.mode('overwrite').parquet(f'{write_path}/order_items')

order_reviews_schema = StructType([
    StructField('review_id', StringType(), True),
    StructField('order_id', StringType(), True),
    StructField('review_score', IntegerType(), True),
    StructField('review_comment_title', StringType(), True),
    StructField('review_comment_message', StringType(),True),
    StructField('review_creation_date', DateType(), True),
    StructField('review_answer_timestamp', DateType(), True)
])

order_reviews_df = spark.read.schema(order_reviews_schema).option('header',True).csv(f'{read_path}/order_reviews.csv')

order_reviews_df = order_reviews_df.drop(col('review_comment_message'))
order_reviews_df.show(5)

order_reviews_df.write.mode('overwrite').parquet(f'{write_path}/order_reviews')


orders_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id',StringType(), True),
    StructField('order_status', StringType(), True),
    StructField('order_purchase_timestamp', DateType(), True),
    StructField('order_approved_timestamp', DateType(), True),
    StructField('order_delivered_carrier_date', DateType(), True),
    StructField('order_delivered_customer_date', DateType(), True),
    StructField('order_estimated_delivery_date', DateType(), True)
    ])

orders_df = spark.read.schema(orders_schema).option('header',True).csv(f'{read_path}/orders.csv')

orders_df = orders_df.filter("order_status = 'delivered'")
orders_df = orders_df.drop(col('order_approved_timestamp'))

orders_df.show(5)

orders_df.write.mode('overwrite').parquet(f'{write_path}/orders')

products_schema = StructType([
    StructField('product_id', StringType(), True),
    StructField('product_category', StringType(), True),
    StructField('product_name_lenght', IntegerType(), True),
    StructField('product_description_lenght', IntegerType(), True),
    StructField('product_photos_qty', IntegerType(), True),
    StructField('product_weight_g', IntegerType(), True),
    StructField('product_length_cm', IntegerType(), True),
    StructField('product_height_cm', IntegerType(), True),
    StructField('product_width_cm', IntegerType(), True)
    ])


products_df = spark.read.schema(products_schema).option('header',True).csv(f'{read_path}/products.csv')
products_df = products_df.drop(col('product_photos_qty'), col('product_length_cm'), col('product_height_cm'), col('product_width_cm'))

products_name_translation_df = spark.read.option('inferSchema','true')\
    .option('header',True).csv(f'{read_path}/product_category_name_translation.csv')


products_transformed_df = products_df.join(broadcast(products_name_translation_df),\
                                           products_df.product_category == products_name_translation_df.product_category_name)\
                                           .drop(col('product_category_name'))\
                                           .drop(col('product_category')).drop(col('product_name_lenght'))\
                                           .drop(col('product_description_lenght')).withColumnRenamed('product_category_name_english', 'category')

products_df.show(5)
products_df.write.mode('overwrite').parquet(f'{write_path}/products')

sellers_schema = StructType([StructField('seller_id', StringType(), True),
                            StructField('seller_zip_code_prefix', StringType(), True),
                            StructField('seller_city', StringType(), True),
                            StructField('seller_state', StringType(), True)])

sellers_df = spark.read.schema(sellers_schema).option('header',True).csv(f'{read_path}/sellers.csv')

sellers_df.write.mode('overwrite').parquet(f'{write_path}/sellers')


order_payments_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('payment_sequential', IntegerType(), True),
    StructField('payment_type', StringType(), True),
    StructField('payment_installments', IntegerType(), True),
    StructField('payment_value', FloatType(), True)
])


order_payments_df = spark.read.schema(order_payments_schema).option('header',True).csv(f'{read_path}/order_payments.csv')
order_payments_df.write.mode('overwrite').parquet(f'{write_path}/order_payments')
order_payments_df.show(5)
spark.stop()