# Data Streaming Pipeline

This project implements a data streaming pipeline that extracts data from AWS Kinesis, processes/transforms the data using Apache Flink, and loads the transformed data into an AWS S3 bucket in real-time. The pipeline simulates data generation using a producer script, processes the data using Flink, and stores the output in S3.

## Features
- **Data Generation**: A Python script (`producer.py`) generates dummy data and streams it into an AWS Kinesis Data Stream.
- **Data Processing**: Apache Flink processes the data in real-time, applying transformations or computations as needed.
- **Data Storage**: Processed data is stored in an AWS S3 bucket for further analysis or use.
- **Configuration**: A `config.json` file is used to define the data stream schema and other runtime configurations.

## Tech Stack
- **Python**: For data generation and pipeline orchestration.
- **AWS Kinesis**: For real-time data streaming.
- **Apache Flink**: For real-time data processing and transformation.
- **AWS S3**: For storing the processed data.

---

## Project Structure
├── config.json # Configuration file for the data stream and pipeline settings 
├── flink-aws-connector.py # Flink application to process data from Kinesis and load it into S3 
├── producer.py # Script to generate dummy data and push it to Kinesis 
└── README.md # Project documentation


---

## Prerequisites
1. **AWS Account**: Ensure you have an active AWS account with permissions to use Kinesis and S3 services.
2. **AWS CLI**: Install and configure the AWS CLI with your credentials.
3. **Python**: Python 3.x installed on your machine.
4. **Apache Flink**: Apache Flink installed and configured on your system or cluster.

---

## Setup and Usage

### 1. Configure the Pipeline
Update the `config.json` file with the required configuration:
- Kinesis stream name
- AWS region
- S3 bucket name
- Data schema for the stream

Example `config.json`:
```json
{
    "kinesis_stream_name": "my-data-stream",
    "aws_region": "us-east-1",
    "s3_bucket_name": "my-s3-bucket",
    "data_schema": {
        "field1": "string",
        "field2": "integer",
        "field3": "timestamp"
    }
}
```

### 2. Generate Dummy Data

Run the `producer.py` script to start generating dummy data and pushing it to the Kinesis stream:

```
python producer.py
```

### 3. Process Data with Flink

Deploy the ```flink-aws-connector.py``` script as a Flink job. This script will:

`Read data from the Kinesis stream
Process or transform the data
Write the processed data to the configured S3 bucket
To run the Flink job:`

```
flink run flink-aws-connector.py
```

### 4. Verify Output

Check your S3 bucket to verify that the processed data files have been uploaded.