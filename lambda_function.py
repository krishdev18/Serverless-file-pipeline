import json
import boto3
import csv
import io
import uuid
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')

# Configuration — replace with your actual values
DYNAMODB_TABLE = 'ProcessedFileResults'
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:243197391447:FileProcessingNotifications'


def lambda_handler(event, context):
    """
    Main handler triggered by S3 upload event.
    event contains bucket name and object key of the uploaded file.
    """

    # Step 1: Extract bucket and file info from the S3 event
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        print(f"Processing file: s3://{bucket_name}/{object_key}")

    except (KeyError, IndexError) as e:
        print(f"Error parsing S3 event: {e}")
        return {'statusCode': 400, 'body': 'Invalid S3 event structure'}

    # Step 2: Read the CSV file from S3
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        print(f"File read successfully. Size: {len(csv_content)} bytes")

    except Exception as e:
        error_msg = f"Failed to read file from S3: {str(e)}"
        print(error_msg)
        publish_sns_notification(object_key, 'FAILED', error_msg)
        return {'statusCode': 500, 'body': error_msg}

    # Step 3: Parse and transform the CSV data
    try:
        processed_rows, summary = parse_and_transform_csv(csv_content)
        print(f"Parsed {len(processed_rows)} rows. Summary: {summary}")

    except Exception as e:
        error_msg = f"Failed to parse CSV: {str(e)}"
        print(error_msg)
        publish_sns_notification(object_key, 'FAILED', error_msg)
        return {'statusCode': 500, 'body': error_msg}

    # Step 4: Write processed results to DynamoDB
    try:
        file_id = str(uuid.uuid4())
        write_to_dynamodb(file_id, object_key, processed_rows, summary)
        print(f"Data written to DynamoDB with file_id: {file_id}")

    except Exception as e:
        error_msg = f"Failed to write to DynamoDB: {str(e)}"
        print(error_msg)
        publish_sns_notification(object_key, 'FAILED', error_msg)
        return {'statusCode': 500, 'body': error_msg}

    # Step 5: Send success notification via SNS
    success_msg = (
        f"File '{object_key}' processed successfully.\n"
        f"Rows processed: {summary['total_rows']}\n"
        f"Rows after filter: {summary['filtered_rows']}\n"
        f"Total sales amount: {summary['total_amount']}\n"
        f"DynamoDB file_id: {file_id}"
    )
    publish_sns_notification(object_key, 'SUCCESS', success_msg)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'File processed successfully',
            'file_id': file_id,
            'summary': summary
        })
    }


def parse_and_transform_csv(csv_content):
    """
    Parses CSV and applies transformations:
    - Filter: only keep rows where Amount > 100
    - Calculate: total amount of filtered rows

    Expected CSV format:
    OrderID, CustomerName, Product, Amount, Status
    """
    reader = csv.DictReader(io.StringIO(csv_content))

    all_rows = []
    filtered_rows = []
    total_amount = 0.0

    for row in reader:
        all_rows.append(row)

        # Transformation: filter rows where Amount > 100
        try:
            amount = float(row.get('Amount', 0))
            if amount > 100:
                filtered_rows.append(row)
                total_amount += amount
        except ValueError:
            # Skip rows with invalid amount values
            print(f"Skipping row with invalid amount: {row}")
            continue

    summary = {
        'total_rows': len(all_rows),
        'filtered_rows': len(filtered_rows),
        'total_amount': round(total_amount, 2)
    }

    return filtered_rows, summary


def write_to_dynamodb(file_id, object_key, rows, summary):
    """
    Writes a summary record + individual row records to DynamoDB.
    """
    table = dynamodb.Table(DYNAMODB_TABLE)
    timestamp = datetime.utcnow().isoformat()

    # Write one summary record for the entire file
    table.put_item(Item={
        'file_id': file_id,
        'timestamp': timestamp,
        'record_type': 'SUMMARY',
        's3_key': object_key,
        'total_rows': summary['total_rows'],
        'filtered_rows': summary['filtered_rows'],
        'total_amount': str(summary['total_amount']),  # DynamoDB needs Decimal or String for floats
        'status': 'PROCESSED'
    })

    # Write individual row records (each gets same file_id, different timestamp suffix)
    for index, row in enumerate(rows):
        row_timestamp = f"{timestamp}#{index}"

        item = {
            'file_id': file_id,
            'timestamp': row_timestamp,
            'record_type': 'ROW',
            's3_key': object_key,
        }

        # Add all CSV columns as DynamoDB attributes
        item.update({k.strip(): str(v).strip() for k, v in row.items()})

        table.put_item(Item=item)


def publish_sns_notification(file_name, status, message):
    """
    Publishes a notification to SNS topic.
    """
    try:
        subject = f"[File Pipeline] {status}: {file_name}"

        full_message = (
            f"Pipeline Status: {status}\n"
            f"File: {file_name}\n"
            f"Time: {datetime.utcnow().isoformat()} UTC\n\n"
            f"Details:\n{message}"
        )

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=full_message
        )
        print(f"SNS notification sent: {status}")

    except Exception as e:
        # Don't let SNS failure crash the whole pipeline
        print(f"Failed to send SNS notification: {e}")