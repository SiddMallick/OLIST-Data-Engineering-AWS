# import findspark
# findspark.init('/home/siddhartha/spark-3.5.1-bin-hadoop3')
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import os
import io
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# AWS credentials
aws_access_key_id=os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key=os.getenv('AWS_SECRET_KEY')

def upload_to_s3(df, s3_client, file_name):

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index = False)

        response = s3_client.put_object(
            Bucket = '<bucket-name>', Key = f'presentation-incremental/{file_name}', Body = csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")   


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.720,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell'

conf = SparkConf().setAppName('S3toSpark')

sc = SparkContext(conf = conf)

spark = SparkSession(sc).builder.appName('S3App').getOrCreate()

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id )
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

read_path = 's3a://<bucket-name>/processed-incremental'

s3_client = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

customer_df = spark.read.parquet(f'{read_path}/customers')
order_payments_df = spark.read.parquet(f'{read_path}/order_payments')
order_reviews_df = spark.read.parquet(f'{read_path}/order_reviews')
order_items_df = spark.read.parquet(f'{read_path}/order_items')
order_df = spark.read.parquet(f'{read_path}/orders')
product_df = spark.read.parquet(f'{read_path}/products')
product_df.show(2)
sellers_df = spark.read.parquet(f'{read_path}/sellers')


customer_df.createOrReplaceTempView("customers")
order_payments_df.createOrReplaceTempView("order_payments")
order_reviews_df.createOrReplaceTempView("order_reviews")
order_items_df.createOrReplaceTempView("order_items")
order_df.createOrReplaceTempView("orders")
product_df.createOrReplaceTempView("products")
sellers_df.createOrReplaceTempView("sellers")

product_df.show(5)

orders_per_day_df = spark.sql('''
      Select DATE(order_purchase_timestamp) as day,
      count(*) as orders
      from orders
      group by day
      order by day
''')

orders_per_day_df.show(10)
orders_per_day = orders_per_day_df.toPandas()

upload_to_s3(orders_per_day, s3_client, 'orders_per_day.csv')


#Order per city
order_per_city_df = customer_df.join(order_df, customer_df.customer_id == order_df.customer_id)\
                               .groupBy('customer_city').agg(count('order_id').alias('order_count'))\
                               .orderBy('order_count', ascending = False)\
                               .select('customer_city', 'order_count').limit(10)

order_per_city_df.show(5)
order_per_city_df = order_per_city_df.toPandas()

upload_to_s3(order_per_city_df, s3_client, 'order_per_city.csv')

category_by_sales_ranked_cte = '''
  select
  pr.product_category_name_english as category,
  round(sum(or_it.price),2) as total_sales,
  rank() over(order by sum(or_it.price) desc) as rank
  from order_items as or_it
  inner join products as pr
  on or_it.product_id = pr.product_id
  group by category
  order by total_sales desc
'''

results_df = spark.sql(f'''
with category_by_sales_ranked as (
  {category_by_sales_ranked_cte}
)
(select
  category,
  total_sales
  from category_by_sales_ranked
  where rank <= 15
  order by total_sales desc)
union
  (select
  'Others',
  round(sum(total_sales),2)
  from category_by_sales_ranked
  where rank > 15)
''')

results_df.show(5)
results_df = results_df.toPandas()

upload_to_s3(results_df, s3_client, 'categorywise_total_sales.csv')

# product_df = product_df.withColumnRenamed('product_category_name_english', 'category')

# product_df.createOrReplaceTempView('products')

category_by_sales_ranked_df = spark.sql(category_by_sales_ranked_cte)
category_top_5 = category_by_sales_ranked_df.select(col('category')).limit(5)

category_top_5.createOrReplaceTempView('category_top_5')

product_category_sales_by_month = spark.sql('''
    select date_format(order_purchase_timestamp, 'yyyy-MM') as year_month,
    round(sum(case when prd.product_category_name_english like 'health_%' then or_it.total_price END),2) as health_beauty,
    round(sum(case when prd.product_category_name_english like 'watches_%' then or_it.total_price END),2) as watches_gifts,
    round(sum(case when prd.product_category_name_english like 'bed_bath_table' then or_it.total_price END),2) as bed_bath_table,
    round(sum(case when prd.product_category_name_english like 'sports_leisure' then or_it.total_price END),2) as sports_leisure,
    round(sum(case when prd.product_category_name_english like 'computer_%' then or_it.total_price END),2) as computer_accessories
    from orders as ord
    join order_items as or_it on ord.order_id = or_it.order_id
    join products as prd on prd.product_id = or_it.product_id
    where prd.product_category_name_english in (select category from category_top_5)
    and date_format(order_purchase_timestamp, 'yyyy-MM') >='2017-01'
    group by year_month
    order by date_format(order_purchase_timestamp, 'yyyy-MM')
''').toPandas()

# Convert 'year_month' column to datetime if it's not already in datetime format
product_category_sales_by_month['year_month'] = pd.to_datetime(product_category_sales_by_month['year_month'])

upload_to_s3(product_category_sales_by_month, s3_client, 'product_category_sales_by_month.csv')


city_by_deliver_time_stats = spark.sql(f'''
  select
  customer_city,
  avg(datediff(order_delivered_carrier_date, order_purchase_timestamp)) as carrier_days,
  avg(datediff(order_delivered_customer_date, order_delivered_carrier_date)) as deliver_days,
  avg(datediff(order_estimated_delivery_date, order_delivered_customer_date)) as delay_days
  from
  orders as ord
  join customers as cust on ord.customer_id = cust.customer_id
  group by customer_city
  having count(order_id) >= 911
  order by carrier_days + deliver_days + delay_days
''').toPandas()

upload_to_s3(city_by_deliver_time_stats, s3_client,'city_by_delivery_time_stats.csv')