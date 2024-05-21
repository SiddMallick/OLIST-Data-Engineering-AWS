from flask import Flask, request, jsonify
import os
import boto3
from dotenv import load_dotenv, dotenv_values 
from flask_cors import CORS
import csv
import pandas as pd

app = Flask(__name__)
CORS(app)

load_dotenv()
s3_client = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_KEY'))

# Define the function to read the CSV file
def read_csv(csv_file):


    # Read the CSV file into a list of rows
    
    input_file = csv.DictReader(open(csv_file))
    json_data = [row for row in input_file]
    # Return the list of rows as a JSON response
    return json_data

def get_csv_data(file_name):
    try:
        file_content = s3_client.get_object(Bucket = 'olist-data-lake', Key=f'presentation/{file_name}')
        csv_file = file_content['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(csv_file.splitlines())
        json_data = [row for row in csv_reader]

        # json_data = read_csv(f'D:/Data_Engineering_Projects/OLIST-Data-Engineering-AWS/presentation/{file_name}')
        # json_data = {'hello': 'hi'}
        return {'file_name':file_name, 'data':json_data}
    
    except:
        return {'message':'Error'}



@app.route('/api/get/<file_name>', methods = ['GET'])
def return_file_route(file_name):
    json_data = get_csv_data(file_name = f'{file_name}.csv')
    if 'message' in json_data:
        return jsonify(json_data), 500
    
    else:
        return jsonify(json_data), 200

@app.route('/api/get/kpis', methods = ['GET'])
def return_kpis():
    try:
        # products_df = pd.read_csv('D:/Data_Engineering_Projects/OLIST-Data-Engineering-AWS/raw/products.csv')
        # customers_df = pd.read_csv('D:/Data_Engineering_Projects/OLIST-Data-Engineering-AWS/raw/customers.csv')
        # reviews_df = pd.read_csv('D:/Data_Engineering_Projects/OLIST-Data-Engineering-AWS/raw/order_reviews.csv')

        # products_df = pd.read_csv('s3://olist-data-lake/raw/products.csv')
        # customers_df = pd.read_csv('s3://olist-data-lake/raw/customers.csv')
        # reviews_df = pd.read_csv('s3://olist-data-lake/raw/order_reviews.csv')

        file_content = s3_client.get_object(Bucket = 'olist-data-lake', Key=f'raw/customers.csv')
        csv_file = file_content['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(csv_file.splitlines())
        json_data = [row for row in csv_reader]

        customers_df = pd.DataFrame(json_data)

        file_content = s3_client.get_object(Bucket = 'olist-data-lake', Key=f'raw/products.csv')
        csv_file = file_content['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(csv_file.splitlines())
        json_data = [row for row in csv_reader]

        products_df = pd.DataFrame(json_data)

        file_content = s3_client.get_object(Bucket = 'olist-data-lake', Key=f'raw/order_reviews.csv')
        csv_file = file_content['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(csv_file.splitlines())
        json_data = [row for row in csv_reader]

        reviews_df = pd.DataFrame(json_data)



        json_data = {'number_of_products': products_df['product_id'].nunique(),
                    'number_of_customers': customers_df['customer_unique_id'].nunique(),
                    'number_of_reviews': reviews_df['review_id'].nunique(),
                    'number_of_orders': customers_df['customer_id'].nunique()}
        
        print(json_data)

        return jsonify(json_data), 200
    except:
        return jsonify({"Message":"Error"}), 200

if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0')