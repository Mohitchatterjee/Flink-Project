
from datetime import datetime, time, timezone
from logging import log
import logging
import random
import threading
import json
import time
import boto3

with open("config.json") as config_file:
    config = json.load(config_file)

def create_kinesis_stream(stream_name):
    """
    Creates an AWS Kinesis Data Stream if it does not already exist.
    This function should be implemented to create the actual stream.
    """
    kinesis_client = boto3.client('kinesis')
    try:
        kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
        logging.info(f"Kinesis stream {stream_name} created successfully.")
    except kinesis_client.exceptions.ResourceInUseException:
        logging.info(f"Kinesis stream {stream_name} already exists.")
    except Exception as e:
        logging.error(f"Error creating Kinesis stream {stream_name}: {e}")

def sent_to_kinesis(kinesis_client, stream_name, data):
    """
    Sends data to AWS Kinesis Data Stream.
    This function should be implemented to send actual data to Kinesis.
    """
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=data['device_id']
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logging.info(f"Data sent to Kinesis stream {stream_name}: {data}")
    else:
        logging.error(f"Failed to send data to Kinesis stream {stream_name}: {response}")

def generator_temperature(device_id):
    """
    Simulates a temperature data generator.
    This function should be implemented to generate actual temperature data.
    """
    return {
        "device_id": device_id,
        "device_type": "temperature",
        "data": round(random.uniform(-10.0, 40.0), 2),
        "unit": "Celsius"
    }

def generator_humidity(device_id):
    """
    Simulates a humidity data generator.
    This function should be implemented to generate actual humidity data.
    """
    return {
        "device_id": device_id,
        "device_type": "humidity",
        "data": round(random.uniform(0.0, 100.0), 2),
        "unit": "Percentage"
    }

def generator_energy(device_id):
    """
    Simulates an energy data generator.
    This function should be implemented to generate actual energy data.
    """
    return {
        "device_id": device_id,
        "device_type": "energy",
        "data": round(random.uniform(0.0, 100.0), 2),
        "unit": "Kilowatt-hour"
    }

def data_generator(device):
    """
    Simulates a data generator that produces data for a given device.
    This function should be implemented to generate actual data.
    """
    device_id = device['id']
    device_type = device['type']
    device_stream = device['stream']

    generator = {
        "temperature": generator_temperature,
        "humidity": generator_humidity,
        "energy": generator_energy,
    }

    generator_func = generator[device_type]
    count = 0
    while count < 50:
        try:
            data = generator_func(device_id)
            kinesis_client = boto3.client('kinesis')
            sent_to_kinesis(kinesis_client, device_stream,data)
            time.sleep(config["interval"])
            logging.info(f"Generated data for device {device_id}: {data}")
            count += 1
        except Exception as e:
            logging.error(f"Error generating data for device {device_id}: {e}")
            time.sleep(config["interval"])

def main():
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    
    for _ in config["devices"]:
        device_stream = _["stream"]
        create_kinesis_stream(device_stream)

    for device in config["devices"]:
        try:
            data_generator(device)
        except Exception as e:
            logging.error(f"Error starting data generator for device {device['id']}: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error occurred in main: {e}")