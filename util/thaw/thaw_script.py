# thaw_script.py
#
# Thaws upgraded (premium) user data
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import sys
import time

from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("thaw_script_config.ini")

"""A16
Initiate thawing of archived objects from Glacier
"""


def initiate_glacier_retrieval(glacier, vault_name, archive_id, job_id, s3_key, sns_topic_arn=None):
    """
    Initiate Glacier archive retrieval with graceful degradation.
    Try Expedited first, fall back to Standard if it fails.
    
    Returns: (restore_job_id, tier) or (None, None) on failure
    """
    # Try Expedited retrieval first
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
    try:
        print(f"[{job_id}] Attempting Expedited retrieval for archive {archive_id}...")
        
        job_params = {
            'Type': 'archive-retrieval',
            'ArchiveId': archive_id,
            'Tier': 'Expedited',
            'Description': f'Expedited retrieval for job {job_id}'
        }
        
        # Add SNS notification if topic ARN is provided
        if sns_topic_arn:
            job_params['SNSTopic'] = sns_topic_arn
            print(f"[{job_id}] SNS notifications will be sent to: {sns_topic_arn}")
        
        response = glacier.initiate_job(
            vaultName=vault_name,
            jobParameters=job_params
        )
        
        restore_job_id = response['jobId']
        print(f"[{job_id}] Expedited retrieval initiated successfully! Job ID: {restore_job_id}")
        print(f"[{job_id}] Your data will be available in a few minutes")
        return restore_job_id, 'Expedited'
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        # Catch the specific error when Expedited retrieval fails due to insufficient capacity
        # https://docs.aws.amazon.com/amazonglacier/latest/dev/api-initiate-job-post.html#api-initiate-job-post-responses-syntax
        if error_code == 'InsufficientCapacityException':
            print(f"[{job_id}] Expedited retrieval failed: Insufficient capacity")
            print(f"[{job_id}] Falling back to Standard retrieval...")
            
            # Fall back to Standard retrieval
            try:
                job_params = {
                    'Type': 'archive-retrieval',
                    'ArchiveId': archive_id,
                    'Tier': 'Standard',
                    'Description': f'Standard retrieval for job {job_id}'
                }
                
                # Add SNS notification if topic ARN is provided
                if sns_topic_arn:
                    job_params['SNSTopic'] = sns_topic_arn
                
                response = glacier.initiate_job(
                    vaultName=vault_name,
                    jobParameters=job_params
                )
                
                restore_job_id = response['jobId']
                print(f"[{job_id}] Standard retrieval initiated successfully! Job ID: {restore_job_id}")
                print(f"[{job_id}] Your data will be available in a few hours")
                return restore_job_id, 'Standard'
                
            except ClientError as se:
                print(f"[{job_id}] Standard retrieval also failed: {se}")
                return None, None
        else:
            print(f"[{job_id}] Expedited retrieval failed with error: {error_code} - {e}")
            return None, None


def update_dynamodb_with_restore_info(dynamodb_table, job_id, restore_job_id, tier):
    """Update DynamoDB with restore job information"""
    try:
        # Update the job item with restore information
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        dynamodb_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET restore_job_id = :rjid, restore_status = :status, restore_tier = :tier, restore_initiated_time = :time',
            ExpressionAttributeValues={
                ':rjid': restore_job_id,
                ':status': 'PENDING',
                ':tier': tier,
                ':time': int(time.time())
            }
        )
        print(f"[{job_id}] Updated DynamoDB with restore job info")
        return True
        
    except ClientError as e:
        print(f"[{job_id}] Error updating DynamoDB: {e}")
        return False


def handle_thaw_queue(sqs=None, queue_url=None, glacier=None, dynamodb_table=None):
    """
    Read messages from the thaw queue and initiate Glacier retrievals
    """
    # Read messages from the queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=int(config.get('sqs', 'MaxMessages')),
            WaitTimeSeconds=int(config.get('sqs', 'WaitTime')),
            MessageAttributeNames=['All']
        )
        
        messages = response.get('Messages', [])
        
        if not messages:
            return  # No messages to process
        
        print(f"\nReceived {len(messages)} message(s) from thaw queue")
        
        # Process messages --> initiate restore from Glacier
        for message in messages:
            try:
                # Parse the message body
                message_body = json.loads(message['Body'])
                
                # Handle SNS-wrapped messages
                if 'Message' in message_body:
                    thaw_data = json.loads(message_body['Message'])
                else:
                    thaw_data = message_body
                
                job_id = thaw_data.get('job_id')
                archive_id = thaw_data.get('results_file_archive_id')
                s3_key = thaw_data.get('s3_key_result_file')
                user_id = thaw_data.get('user_id')
                
                if not all([job_id, archive_id, s3_key]):
                    print(f"Missing required fields in message: {thaw_data}")
                    # Delete malformed message
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    continue
                
                print(f"\nProcessing thaw request for job {job_id} (user: {user_id})")
                
                # Get vault name from config
                vault_name = config.get('glacier', 'VaultName')
                
                # Get SNS topic ARN for Glacier completion notifications
                sns_topic_arn = None
                if config.has_option('sns', 'GlacierRestoreTopic'):
                    sns_topic_arn = config.get('sns', 'GlacierRestoreTopic')
                
                # Initiate Glacier retrieval (with graceful degradation)
                restore_job_id, tier = initiate_glacier_retrieval(
                    glacier, vault_name, archive_id, job_id, s3_key, sns_topic_arn
                )
                
                # Only proceed if Glacier retrieval was initiated successfully
                if restore_job_id:
                    # Update DynamoDB with restore job information
                    dynamodb_success = update_dynamodb_with_restore_info(
                        dynamodb_table, job_id, restore_job_id, tier
                    )
                    
                    if dynamodb_success:
                        print(f"[{job_id}] Thaw request processed successfully")
                        
                        # Delete message from queue only after BOTH operations succeed
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print(f"[{job_id}] Message deleted from queue")
                    else:
                        # DynamoDB update failed - don't delete message so it can be retried
                        print(f"[{job_id}] DynamoDB update failed - message will be retried")
                        # Note: Glacier job is already initiated, but without DB record
                        # On retry, DynamoDB update will be attempted again
                else:
                    # Glacier retrieval failed - delete message as it's likely a permanent error
                    print(f"[{job_id}] Failed to initiate Glacier retrieval")
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"[{job_id}] Message deleted from queue (retrieval failed)")
                
            except json.JSONDecodeError as e:
                print(f"Error parsing message JSON: {e}")
                # Delete malformed message
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except Exception as e:
                print(f"Error processing message: {e}")
                # Don't delete the message so it can be retried
                
    except ClientError as e:
        print(f"Error receiving messages from SQS: {e}")
    except Exception as e:
        print(f"Unexpected error in handle_thaw_queue: {e}")


def main():
    """Main function to poll thaw queue and process requests"""
    
    # Get handles to resources
    region = config.get('aws', 'AwsRegionName')
    
    # Initialize AWS clients
    sqs = boto3.client('sqs', region_name=region)
    glacier = boto3.client('glacier', region_name=region)
    dynamodb = boto3.resource('dynamodb', region_name=region)
    
    # Get queue URL
    queue_name = config.get('sqs', 'ThawQueueName')
    try:
        queue_url_response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = queue_url_response['QueueUrl']
        print(f"Thaw script started. Polling queue: {queue_name}")
        print(f"Queue URL: {queue_url}")
    except ClientError as e:
        print(f"Error: Could not find SQS queue '{queue_name}': {e}")
        sys.exit(1)
    
    # Get DynamoDB table
    table_name = config.get('gas', 'AnnotationsTable')
    table = dynamodb.Table(table_name)
    print(f"Using DynamoDB table: {table_name}")
    print(f"Using Glacier vault: {config.get('glacier', 'VaultName')}")
    print("\nWaiting for thaw requests...\n")
    
    # Poll queue for new results and process them
    while True:
        handle_thaw_queue(
            sqs=sqs,
            queue_url=queue_url,
            glacier=glacier,
            dynamodb_table=table
        )


if __name__ == "__main__":
    main()

### EOF
