import psycopg2
import csv
import boto3

import os 


host = 'localhost'
dbname = 'olist'
user = 'postgres'
password = 'your-password'
port = '5432'

def delete_csv(file):
    if(os.path.exists(file) and os.path.isfile(file)): 
        os.remove(file) 
        print("file deleted") 
    else: 
        print("file not found") 

def upload_file_to_s3(s3, s3_bucket_name, s3_folder_name, file_name):
    #Upload
    with open(file_name, 'rb') as f:
        s3.upload_fileobj(f, s3_bucket_name, s3_folder_name + file_name)

def export_table_to_csv(s3, s3_bucket_name, s3_folder_name, table_name, csv_file_name):
    try:
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        
        # Query the database to select all rows from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Write rows to a CSV file
        with open(csv_file_name, 'w', newline='',encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([desc[0] for desc in cursor.description])  # Write column headers
            csv_writer.writerows(rows)  # Write rows
        
        print(f"Data exported from table '{table_name}' to '{csv_file_name}' successfully.")
    
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    
    finally:
        # Close the cursor and connection
        if conn is not None:
            conn.close()

    print('Uploading csv to s3')
    upload_file_to_s3(s3, s3_bucket_name, s3_folder_name, csv_file_name)
    print(f'Deleting {csv_file_name}')
    delete_csv(csv_file_name)


s3= boto3.client('s3', aws_access_key_id='your-access-key', aws_secret_access_key='your-secret-key')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','customers', 'customers.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','geolocation', 'geolocation.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','order_items', 'order_items.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','order_payments', 'order_payments.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','order_reviews', 'order_reviews.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','orders', 'orders.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','product_category_name_translation', 'product_category_name_translation.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','products', 'products.csv')
export_table_to_csv(s3, 'sidd-rds-to-s3-olist', 'raw/','sellers', 'sellers.csv')


