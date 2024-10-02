from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os
import logging
from botocore.exceptions import ClientError


# Function to read AWS credentials from .ini file and upload to S3
def upload_to_s3(bucket_name, key, file_path):
    # Create a Boto3 session
    session = boto3.Session(
        aws_access_key_id='123456',  # Replace with your access key
        aws_secret_access_key='123456',  # Replace with your secret key
        region_name='us-east-1'  # Specify your region
    )
    
    s3 = session.resource('s3', endpoint_url='http://localhost:4566')
        
     # Check if the bucket already exists
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            s3.create_bucket(Bucket=bucket_name)
    else:
        logging.info(f"Bucket '{bucket_name}' already exists.")
        raise Exception(f"Bucket '{bucket_name}' already exists.")


    with open(file_path, 'rb') as jsonFile:
        s3.Object(bucket_name, key).put(Body=jsonFile.read())

    logging.info(f"File '{file_path}' uploaded to bucket '{bucket_name}' with key '{key}'.")

# Define the Airflow DAG
with DAG(
    'sample_s3_dag',
    start_date=datetime(2023, 9, 27),
    schedule=None,
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_s3_ini',
        python_callable=upload_to_s3,
        op_kwargs={
            'bucket_name': 'my-local-bucket',
            'key': 'sample_data.json',
            'file_path': 'sample_data.json'
        }
    )

    

