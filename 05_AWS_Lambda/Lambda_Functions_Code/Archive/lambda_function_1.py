import boto3
import pandas as pd
from io import BytesIO
import time

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    s3_file_name = 'output/thehoxtontrend/YouTube_Statistics_Data/YouTube_Statistics_Table2021_05_11.csv'
    #'output/thehoxtontrend/YouTube_Statistics_Data/YouTube_Statistics_Table'+time.strftime("%Y_%m_%d") + '.csv'
    bucket_name = 'ucl-msin0166-2021-mwaa' 
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)

    data = pd.read_csv(resp['Body'], sep=';')
    
    json_data = data.to_json()
    # Method 2
    # df_s3_data = pd.read_csv(BytesIO(resp['Body'].read().decode('utf-8')))

    return json_data