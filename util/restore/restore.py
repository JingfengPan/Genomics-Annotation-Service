# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##

import boto3
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

# Define constants here; no config file is used for Lambdas
REGION = "us-east-1"
DYNAMODB_TABLE = "jfpan01_annotations"
S3_RESULTS_BUCKET = "gas-results"
GLACIER_VAULT = "ucmpcs"

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=REGION)
glacier_client = boto3.client('glacier', region_name=REGION)
dynamodb_resource = boto3.resource('dynamodb', region_name=REGION)
dynamodb_table = dynamodb_resource.Table(DYNAMODB_TABLE)


def get_job_info_from_dynamodb(job_id):
    """
    Retrieve job information from DynamoDB
    """
    try:
        response = dynamodb_table.get_item(Key={'job_id': job_id})
        
        if 'Item' in response:
            return response['Item']
        else:
            print(f"Job {job_id} not found in DynamoDB")
            return None
            
    except ClientError as e:
        print(f"Error retrieving job from DynamoDB: {e}")
        return None


def download_from_glacier(vault_name, restore_job_id):
    """
    Download the thawed archive from Glacier
    Returns: archive data bytes or None on failure
    """
    try:
        print(f"Downloading archive from Glacier job: {restore_job_id}")
        
        # Get the job output (the restored archive)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
        response = glacier_client.get_job_output(
            vaultName=vault_name,
            jobId=restore_job_id
        )
        
        # Read the archive data from the streaming body
        archive_data = response['body'].read()
        
        print(f"Successfully downloaded {len(archive_data)} bytes from Glacier")
        return archive_data
        
    except ClientError as e:
        print(f"Error downloading from Glacier: {e}")
        return None


def upload_to_s3(bucket, key, data):
    """
    Upload the restored data to S3
    """
    try:
        print(f"Uploading to S3: s3://{bucket}/{key}")
        
        # Upload to S3 with server-side encryption
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ServerSideEncryption='AES256'
        )
        
        print(f"Successfully uploaded to S3")
        return True
        
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        return False


def delete_glacier_archive(vault_name, archive_id):
    """
    Delete the archive from Glacier after successful restoration
    """
    try:
        print(f"Deleting archive from Glacier: {archive_id}")
        
        # Delete the archive
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
        glacier_client.delete_archive(
            vaultName=vault_name,
            archiveId=archive_id
        )
        
        print(f"Successfully deleted archive from Glacier")
        return True
        
    except ClientError as e:
        print(f"Error deleting archive from Glacier: {e}")
        # Don't fail the entire process if deletion fails
        return False


def update_dynamodb_after_restore(job_id):
    """
    Update DynamoDB to remove restoration-related fields
    """
    try:
        print(f"Updating DynamoDB for job {job_id}")
        
        # Remove all restoration-related fields
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        dynamodb_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='REMOVE results_file_archive_id, restore_job_id, restore_status, restore_tier, restore_initiated_time'
        )
        
        print(f"Successfully updated DynamoDB")
        return True
        
    except ClientError as e:
        print(f"Error updating DynamoDB: {e}")
        return False


def lambda_handler(event, context):
    """
    Lambda function handler - triggered when Glacier restoration completes
    """
    print("Received event: " + json.dumps(event, indent=2))
    
    try:
        # Parse the event to extract message data
        # Handle different event formats
        
        restore_job_id = None
        job_id = None
        
        if 'Records' in event and len(event['Records']) > 0:
            # SNS trigger from Glacier
            sns_record = event['Records'][0]['Sns']
            message_str = sns_record['Message']
            
            # Check if this is a Glacier job completion notification
            # Glacier sends JSON with fields like: Action, ArchiveId, JobId, StatusCode, etc.
            try:
                glacier_message = json.loads(message_str)
                
                # Glacier notification format
                if 'JobId' in glacier_message and 'Action' in glacier_message:
                    print("Detected Glacier job completion notification")
                    restore_job_id = glacier_message['JobId']
                    status_code = glacier_message.get('StatusCode', '')
                    
                    if status_code != 'Succeeded':
                        print(f"Glacier job status: {status_code} (not Succeeded), skipping")
                        return {
                            'statusCode': 200,
                            'body': json.dumps(f'Glacier job not successful: {status_code}')
                        }
                    
                    # Need to find the corresponding job_id in DynamoDB using restore_job_id
                    # Query DynamoDB for job with this restore_job_id
                    try:
                        response = dynamodb_table.scan(
                            FilterExpression='restore_job_id = :rjid',
                            ExpressionAttributeValues={':rjid': restore_job_id}
                        )
                        
                        if response['Items']:
                            job_id = response['Items'][0]['job_id']
                            print(f"Found job_id {job_id} for restore_job_id {restore_job_id}")
                        else:
                            print(f"No job found with restore_job_id {restore_job_id}")
                            return {
                                'statusCode': 404,
                                'body': json.dumps('No job found for this restore_job_id')
                            }
                    except ClientError as e:
                        print(f"Error querying DynamoDB: {e}")
                        return {
                            'statusCode': 500,
                            'body': json.dumps('Error finding job in database')
                        }
                
                # Manual invocation format (our custom message)
                elif 'job_id' in glacier_message:
                    job_id = glacier_message.get('job_id')
                    restore_job_id = glacier_message.get('restore_job_id')
                
            except (json.JSONDecodeError, KeyError):
                print("Could not parse as Glacier or custom message")
                return {
                    'statusCode': 400,
                    'body': json.dumps('Invalid message format')
                }
        elif 'Message' in event:
            # SNS message directly
            message_data = json.loads(event['Message'])
            job_id = message_data.get('job_id')
            restore_job_id = message_data.get('restore_job_id')
        else:
            # Direct invocation with job data
            job_id = event.get('job_id')
            restore_job_id = event.get('restore_job_id')
        
        if not job_id or not restore_job_id:
            print(f"Missing required fields: job_id={job_id}, restore_job_id={restore_job_id}")
            return {
                'statusCode': 400,
                'body': json.dumps('Missing required fields')
            }
        
        print(f"Processing restoration for job {job_id}, Glacier job {restore_job_id}")
        
        # Get job information from DynamoDB
        job_info = get_job_info_from_dynamodb(job_id)
        
        if not job_info:
            return {
                'statusCode': 404,
                'body': json.dumps(f'Job {job_id} not found')
            }
        
        # Extract needed information
        archive_id = job_info.get('results_file_archive_id')
        s3_key = job_info.get('s3_key_result_file')
        
        if not archive_id or not s3_key:
            print(f"Missing archive_id or s3_key in job info")
            return {
                'statusCode': 400,
                'body': json.dumps('Missing archive or S3 key information')
            }
        
        # Download the thawed archive from Glacier
        archive_data = download_from_glacier(GLACIER_VAULT, restore_job_id)
        
        if not archive_data:
            print(f"Failed to download archive from Glacier")
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to download from Glacier')
            }
        
        # Upload to S3 results bucket
        if not upload_to_s3(S3_RESULTS_BUCKET, s3_key, archive_data):
            print(f"Failed to upload to S3")
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to upload to S3')
            }
        
        # Delete the archive from Glacier
        delete_glacier_archive(GLACIER_VAULT, archive_id)
        
        # Update DynamoDB to remove restoration fields
        update_dynamodb_after_restore(job_id)
        
        print(f"Successfully completed restoration for job {job_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully restored job {job_id}')
        }
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps(f'JSON parse error: {str(e)}')
        }
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


### EOF
