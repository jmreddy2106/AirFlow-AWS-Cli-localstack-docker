import pytest
from airflow.models import DagBag
from unittest import mock
from dags.s3_dag import upload_to_s3
from helper import load_aws_config
import boto3
from moto import mock_aws
import logging
from botocore.exceptions import ClientError

# Sample data for testing
BUCKET_NAME = 'my-local-bucket'
KEY = 'sample_data.json'
FILE_PATH = 'sample_data.json'

@pytest.fixture(scope="module", autouse=True)
def setup_aws_config():
    # Load AWS credentials from the .ini file for the default profile
    load_aws_config(config_file='aws_localstack_config.ini', profile='default')

# Load the Airflow DAG
@pytest.fixture
def dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dag_bag):
    """Ensure that the DAG is properly loaded."""
    dag = dag_bag.get_dag(dag_id='sample_s3_dag')
    assert dag is not None
    assert len(dag.tasks) == 1

@mock_aws
def test_upload_to_s3():
    """Test the upload_to_s3 function."""
    # Set up the mock S3 environment
    s3 = boto3.resource('s3', endpoint_url='http://localhost:4566')

    # Cleanup to ensure the test starts with no existing buckets
    for bucket in s3.buckets.all():
        bucket.objects.all().delete()
        bucket.delete()
                 
    # Call the function to upload the sample file to S3
    upload_to_s3(BUCKET_NAME, KEY, FILE_PATH)

    # Verify the file was uploaded
    body = s3.Object(BUCKET_NAME, KEY).get()['Body'].read().decode('utf-8')
    assert body == '{"ID": 123456}'  # Check if the content matches

@mock_aws
def test_upload_to_s3_bucket_already_exists():
    """Test that an exception is raised when trying to create an already existing bucket."""
    
    # Set up the mock S3 environment and create the bucket
    s3 = boto3.resource('s3', endpoint_url='http://localhost:4566')
    
     # Create the bucket first
    s3.create_bucket(Bucket=BUCKET_NAME)

    for bucket in s3.buckets.all():
        logging.info(f"Available Bucket - {bucket.name}")

    # # Call the function to upload the sample file to S3
    # upload_to_s3(BUCKET_NAME, KEY, FILE_PATH)
    
    # Now, trying to upload again should raise the "BucketAlreadyExists" exception
    with pytest.raises(Exception) as exc_info:
        upload_to_s3(BUCKET_NAME, KEY, FILE_PATH)
    
    # Check that the exception message matches
    assert str(exc_info.value) == f"Bucket '{BUCKET_NAME}' already exists."

@mock_aws
def test_upload_to_s3_non_existent_file():
    """Test the behavior when trying to upload a non-existent file."""

    # Cleanup any existing buckets
    s3 = boto3.resource('s3', endpoint_url='http://localhost:4566')
    
    # Create the bucket
    s3.create_bucket(Bucket=BUCKET_NAME)

    NON_KEY = "non_sample_data.json"
    # Check if the file exists in the bucket
    try:
        s3.Object(BUCKET_NAME, NON_KEY).load()  # Load the object metadata to check existence
        file_exists = False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            file_exists = True
        else:
            raise

    # Assert that the file exists in the bucket
    assert file_exists, f"File '{NON_KEY}' does not exist in bucket '{BUCKET_NAME}'."


