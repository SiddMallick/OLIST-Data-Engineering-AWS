# import findspark
# findspark.init('/home/ubuntu/spark-3.5.1-bin-hadoop3')
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

load_dotenv()

#Mention the date of the load
date_of_load = '<input-preferred-date>'

# AWS credentials
aws_access_key_id=os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key=os.getenv('AWS_SECRET_KEY')

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

#Mention the appropriate bucket names and key names for the raw and presentation zones

try:
    customer_df = spark.read.schema(customer_schema).option('header', 'true')\
        .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/customers.csv')

    try:
        existing_customer_df = spark.read.parquet('s3a://<bucket-name + processed-bucket-key-name>/customers')
        customer_df = customer_df.join(existing_customer_df, existing_customer_df.customer_id == customer_df.customer_id, 'left_anti')
        print(customer_df.count())
        customer_df.show()
        customer_df.repartition(1).write.option("compression", "snappy")\
        .mode('append').parquet('s3a://<bucket-name + processed-bucket-key-name>/customers')
        print('Appended data to customers')
    except:
        print(customer_df.count())
        customer_df.show(3)
        customer_df.repartition(1).write.option("compression", "snappy").mode('overwrite')\
            .parquet('s3a://<bucket-name + processed-bucket-key-name>/customers')
        print('First load for customers')
except:
    print("File not found")


order_items_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('order_item_id', IntegerType(), True),
    StructField('product_id', StringType(), True),
    StructField('seller_id', StringType(), True),
    StructField('shipping_limit_date', TimestampType(), True),
    StructField('price', FloatType(), True),
    StructField('freight_value', FloatType(), True)
])

try:
    order_items_df = spark.read.schema(order_items_schema).option('header', 'true')\
        .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/order_items.csv')

    try:
        existing_order_items_df = spark.read.parquet('s3a://<bucket-name + processed-bucket-key-name>/order_items')
        # Perform left anti join to find rows in new_data that are not in existing_data
        incremental_data = order_items_df.join(existing_order_items_df,
                                on=['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price',
                                    'freight_value'], how='left_anti')\
                                    .withColumn('total_price', col('price')*col('order_item_id') + col('freight_value')*col('order_item_id'))
        print(incremental_data.count())
        incremental_data.show()
        incremental_data.repartition(1).write.option("compression", "snappy").mode('append')\
            .parquet('s3a://<bucket-name + processed-bucket-key-name>/order_items')

        print('Appended data to order_items')
    except:
        order_items_df = order_items_df.withColumn('total_price', col('price')*col('order_item_id') + col('freight_value')*col('order_item_id'))
        print(order_items_df.count())
        order_items_df.show()
        order_items_df.repartition(1).write.option("compression", "snappy").mode('overwrite')\
            .parquet('s3a://<bucket-name + processed-bucket-key-name>/order_items')
        print('First load for order_items')
except:
    print("File not found")


order_payments_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('payment_sequential', IntegerType(), True),
    StructField('payment_type', StringType(), True),
    StructField('payment_installments', IntegerType(), True),
    StructField('payment_value', FloatType(), True)
])

try:
    order_payments_df = spark.read.schema(order_payments_schema).option('header', 'true')\
        .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/order_payments.csv')

    try:
        existing_order_payments_df = spark.read.parquet('s3a://<bucket-name + processed-bucket-key-name>/order_payments')
        # Perform left anti join to find rows in new_data that are not in existing_data
        incremental_data = order_payments_df.join(existing_order_payments_df,
                                on=['order_id', 'payment_sequential','payment_type', 'payment_installments','payment_value'], how='left_anti')

        print(incremental_data.count())
        incremental_data.show()
        incremental_data.repartition(1).write.option("compression", "snappy").mode('append').parquet('s3a://<bucket-name + processed-bucket-key-name>/order_payments')

        print('Appended data to order_payments')
    except:
        print(order_payments_df.count())
        order_payments_df.show()
        order_payments_df.repartition(1).write.option("compression", "snappy")\
            .mode('overwrite').parquet('s3a://<bucket-name + processed-bucket-key-name>/order_payments')

        print('First load for order_payments')
except:
    print("File not found")



order_reviews_schema = StructType([
    StructField('review_id', StringType(), True),
    StructField('order_id', StringType(), True),
    StructField('review_score', IntegerType(), True),
    StructField('review_comment_title', StringType(), True),
    StructField('review_comment_message', StringType(),True),
    StructField('review_creation_date', TimestampType(), True),
    StructField('review_answer_timestamp', TimestampType(), True)
])


try:
  order_reviews_df = spark.read.schema(order_reviews_schema).option('header', 'true')\
  .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/order_reviews.csv')

  try:
    existing_order_reviews_df = spark.read.parquet('s3a://<bucket-name + processed-bucket-key-name>/order_reviews')
    # Perform left anti join to find rows in new_data that are not in existing_data
    order_reviews = order_reviews_df.join(existing_order_reviews_df,
                            on=['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_creation_date', 'review_answer_timestamp'],
                            how='left_anti')

    print(order_reviews.count())
    order_reviews.show()
    order_reviews.repartition(1).write\
        .option("compression", "snappy").mode('append').parquet('s3a://<bucket-name + processed-bucket-key-name>/order_reviews')
    print('Appended order_reviews')
  except:
    print(order_reviews_df.count())
    order_reviews_df.show()
    order_reviews_df.repartition(1).write\
        .option("compression", "snappy").mode('overwrite').parquet('s3a://<bucket-name + processed-bucket-key-name>/order_reviews')
    print('Written order_reviews data')
except:
  print('No file found')


orders_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id',StringType(), True),
    StructField('order_status', StringType(), True),
    StructField('order_purchase_timestamp', TimestampType(), True),
    StructField('order_approved_timestamp', TimestampType(), True),
    StructField('order_delivered_carrier_date', TimestampType(), True),
    StructField('order_delivered_customer_date', TimestampType(), True),
    StructField('order_estimated_delivery_date', TimestampType(), True)
    ])

try:
  orders_df = spark.read.schema(orders_schema).option('header', 'true')\
  .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/orders.csv').drop(col('oder_delivered_carrier_date'))

  try:
    existing_orders_df = spark.read.parquet('s3a://<bucket-name + processed-bucket-key-name>/orders')
    # Perform left anti join to find rows in new_data that are not in existing_data
    orders = orders_df.join(existing_orders_df,
                            on=['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_timestamp','order_delivered_customer_date',
                                'order_estimated_delivery_date'],
                            how='left_anti').filter("order_status = 'delivered' ")

    print(orders.count())
    orders.show()
    orders.repartition(1).write.option("compression", "snappy").mode('append')\
        .parquet('s3a://<bucket-name + processed-bucket-key-name>/orders')
    print('apended orders data')

  except:
    print(orders_df.count())
    orders_df.show()
    orders_df.repartition(1).write\
        .option("compression", "snappy").mode('overwrite').parquet('s3a://<bucket-name + processed-bucket-key-name>/orders')
    print('Written orders data')
except:
  print('No file found')


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

try:

  products_df = spark.read.schema(products_schema).option('header', 'true')\
  .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/products.csv')

  products_df = products_df.drop(col('product_photos_qty'), col('product_length_cm'), col('product_height_cm'), col('product_width_cm'))

  products_name_translation_df = spark.read.option('inferSchema', 'true').option('header', 'true')\
      .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/product_category_name_translation.csv')

  products_transformed_df = products_df.join(broadcast(products_name_translation_df),\
                                            products_df.product_category == products_name_translation_df.product_category_name)\
                                            .drop(col('product_category_name'))\
                                            .drop(col('product_category')).drop(col('product_name_lenght'))\
                                            .drop(col('product_description_lenght'))
  products_transformed_df.count()

  products_transformed_df.repartition(1).write.mode('overwrite')\
      .parquet('s3a://<bucket-name + processed-bucket-key-name>/products')
  
  print('Written products data')
except:
  print('No file found')


sellers_schema = StructType([StructField('seller_id', StringType(), True),
                            StructField('seller_zip_code_prefix', StringType(), True),
                            StructField('seller_city', StringType(), True),
                            StructField('seller_state', StringType(), True)])


try:

  sellers_df = spark.read.schema(sellers_schema).option('header', 'true')\
  .csv(f's3a://<bucket-name + raw bucket key name>/{date_of_load}/sellers.csv')

  sellers_df.count()

  sellers_df.repartition(1).write.mode('overwrite')\
      .parquet('s3a://<bucket-name + processed-bucket-key-name>/sellers')
  
  print('Written Sellers data')
except:
  print('No file found')


spark.stop()