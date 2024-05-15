import pandas as pd


def csv_to_json(csv_file_path):
    # Open the CSV file for reading
    df = pd.read_csv(csv_file_path)
    # Convert DataFrame to JSON
    json_data = df.to_json(orient='records')
    
    return json_data

json_data = csv_to_json(f'D://Data_Engineering_Projects/OLIST-Data-Engineering-AWS/presentation/orders_per_day.csv')
print(json_data)