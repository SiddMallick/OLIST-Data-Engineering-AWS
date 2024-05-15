
# aws_session_token = ''


# # Create a Spark session with your AWS Credentials

# from pyspark import SparkConf
# from pyspark.sql import SparkSession
 
# conf = SparkConf()
# conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
# conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
# conf.set('spark.hadoop.fs.s3a.access.key', aws_access_key_id)
# conf.set('spark.hadoop.fs.s3a.secret.key',aws_secret_access_key)

 
# spark = SparkSession.builder.config(conf=conf).getOrCreate()

# df = spark.read.format('csv').load('s3a://olist-data-lake/raw/customers.csv')
# df.show(5, truncate=False)

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

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

df = spark.read.format('csv').load('s3a://olist-data-lake/raw/customers.csv')