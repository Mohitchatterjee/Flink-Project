import os
import boto3
import json
from botocore.exceptions import ClientError
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes, RowType, RowField
from pyflink.common import Configuration
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import logging


def create_s3_bucket(bucket_name, region_name):
    """
    Creates an S3 bucket if it does not exist.
    """
    s3_client = boto3.client('s3', region_name=region_name)
    try:
        # Check if the bucket already exists
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"S3 bucket '{bucket_name}' already exists.")
    except ClientError as e:
        # If the bucket does not exist, create it
        if e.response['Error']['Code'] == '404':
            logging.info(f"S3 bucket '{bucket_name}' does not exist. Creating...")
            try:
                # Specify LocationConstraint for regions other than ap-south-1
                if region_name == os.getenv('AWS_REGION'):
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region_name}
                    )
                logging.info(f"S3 bucket '{bucket_name}' created successfully.")
            except ClientError as e:
                logging.error(f"Error creating bucket: {e}")
                raise
        else:
            logging.error(f"Error checking bucket: {e}")
            raise

def read_kinesis_stream(stream_name, region_name, shard_id=os.getenv("SHARD_ID")):
    try:
        """Read data from Kinesis using boto3."""
        client = boto3.client('kinesis', region_name=region_name)
        shard_iterator = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON"  # Start from the earliest record
        )["ShardIterator"]

        count = 0
        while count < 50:  # Limit to 50 iterations for demonstration
            response = client.get_records(ShardIterator=shard_iterator, Limit=5)
            shard_iterator = response["NextShardIterator"]
            records = response.get("Records", [])
            for record in records:
                yield json.loads(record["Data"])
                count += 1

    except Exception as e:
        logging.error(f"Error reading from Kinesis stream {stream_name}: {e}")
        raise
    

def upload_dataframe_to_s3(df, bucket_name, object_name, region_name=None):
    """
    Uploads a pandas DataFrame as a CSV file to an S3 bucket without saving it locally.

    :param df: The pandas DataFrame to upload.
    :param bucket_name: The name of the S3 bucket.
    :param object_name: The name of the object in the bucket.
    :param region_name: AWS region where the bucket is located.
    :return: True if the upload was successful, False otherwise.
    """
    # Convert the DataFrame to a CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)  # Write the DataFrame to the buffer as CSV

    # Create an S3 client
    s3_client = boto3.client("s3", region_name=region_name)
    
    try:
        # Upload the CSV string to S3
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        logging.info(f"DataFrame uploaded to S3 bucket '{bucket_name}' as '{object_name}'.")
        return True
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False

def main():
    try:

        # Load environment variables from the .env file
        load_dotenv()

        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s')

        # Read from Kinesis using boto3
        stream_name = os.getenv("STREAM_NAME")
        region_name = os.getenv("AWS_REGION")
        bucket_name = os.getenv("BUCKET_NAME")
        kinesis_data = list(read_kinesis_stream(stream_name, region_name))

        # Create a PyFlink TableEnvironment
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = TableEnvironment.create(env_settings)

    
        # Define a schema for the data
        schema = RowType([
            RowField("device_id", DataTypes.STRING()),
            RowField("device_type", DataTypes.STRING()),
            RowField("data", DataTypes.DOUBLE()),
            RowField("unit", DataTypes.STRING())
        ])

        # Create a DataStream or Table to process the data
        data = [(item["device_id"], item["device_type"], item["data"], item["unit"]) for item in kinesis_data]

        # Create a temporary table from the data
        temp_view = t_env.from_elements(data, schema)
        print(temp_view.get_schema())

        # Register the temporary view
        t_env.create_temporary_view("temp_view", temp_view)

        # Perform transformations or queries on the temporary view
        logging.info("========== Data Transformation Started ==========")
        transformed_data = t_env.sql_query("SELECT * FROM temp_view where data > -10.0 and data < 10.0")
        logging.info("========== Data Transformation Completed ==========")

        # Convert the transformed data to a pandas DataFrame
        table = transformed_data.to_pandas()
        logging.info(f"Transformed DataFrame (first 50 rows):\n{table.head(50)}")

        create_s3_bucket(bucket_name, region_name)


        # Upload the DataFrame to S3
        success = upload_dataframe_to_s3(table, bucket_name, os.getenv("BUCKET_OBJECT_NAME"), region_name)

        if success:
            logging.info("File uploaded successfully.")
        else:
            logging.error("File upload failed.")
    except Exception as e:
        logging.error(f"Credentials error: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error occurred in main: {e}")