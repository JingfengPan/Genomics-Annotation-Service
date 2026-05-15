# run.py
#
# Runs the AnnTools pipeline
#
# NOTE: This file lives on the AnnTools instance and
# replaces the default AnnTools run.py
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import sys
import time
import driver
import boto3
import os
import shutil
import json
from botocore.exceptions import ClientError

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")


# Helper function to get SNS topic ARN by name
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.list_topics
def get_topic_arn(topic_name, region):
    try:
        sns = boto3.client('sns', region_name=region)
        # Handle pagination - list_topics can return paginated results
        paginator = sns.get_paginator('list_topics')
        for page in paginator.paginate():
            topics = page.get('Topics', [])
            for topic in topics:
                arn = topic['TopicArn']
                if arn.endswith(':' + topic_name):
                    return arn
        print(f"Warning: SNS topic {topic_name} not found")
        return None
    except Exception as e:
        print(f"Error finding SNS topic: {str(e)}")
        return None


"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


def main():

    if len(sys.argv) <= 1:
        print("A valid .vcf file must be provided as input to this program.")
        return

    # Get job parameters
    input_file_name = sys.argv[1]

    # Run the AnnTools pipeline
    # Call the AnnTools pipeline
    with Timer() as t:
        driver.run(input_file_name, "vcf")

    # Get configuration values
    region = config.get("aws", "AwsRegionName")
    results_bucket = config.get("s3", "ResultsBucketName")
    s3_key_prefix = config.get("s3", "KeyPrefix")
    ddb_table = config.get("gas", "AnnotationsTable")
    status_completed = config.get("job_status", "Completed")
    status_running = config.get("job_status", "Running")
    archive_topic = config.get("sns", "ArchiveTopic")

    try:
        s3 = boto3.client("s3", region_name=region)
        ddb = boto3.resource("dynamodb", region_name=region)

        # Derive paths & names from the input file
        input_path = os.path.abspath(input_file_name)
        job_dir = os.path.dirname(input_path)
        job_id = os.path.basename(job_dir)
        base = os.path.splitext(os.path.basename(input_path))[0]

        result_file = os.path.join(job_dir, f"{base}.annot.vcf")
        log_file = os.path.join(job_dir, f"{base}.vcf.count.log")

        # Get the user_id and user_email from the DynamoDB job record
        # This ensures we use the authenticated user's ID, not a hardcoded value
        table = ddb.Table(ddb_table)
        try:
            response = table.get_item(Key={"job_id": job_id})
            job_item = response.get("Item")
            if job_item and "user_id" in job_item:
                user_id = job_item["user_id"]
                user_email = job_item.get("user_email", "")  # Get email for notifications
            else:
                print(f"[ERROR] Could not retrieve user_id from job {job_id}")
                return
        except Exception as e:
            print(f"[ERROR] Failed to fetch job from DynamoDB: {e}")
            return

        # S3 keys: <CNetID>/<user_id>/<job_id>/...
        # user_id is the Globus Auth UUID from the authenticated user
        result_key = f"{s3_key_prefix}{user_id}/{job_id}/{base}.annot.vcf"
        log_key = f"{s3_key_prefix}{user_id}/{job_id}/{base}.vcf.count.log"

        # 1. Upload the results file to S3 results bucket
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html
        if os.path.exists(result_file):
            s3.upload_file(result_file, results_bucket, result_key)
            print(f"Uploaded result to s3://{results_bucket}/{result_key}")
        else:
            print(f"Result file not found: {result_file}")

        # 2. Upload the log file to S3 results bucket
        if os.path.exists(log_file):
            s3.upload_file(log_file, results_bucket, log_key)
            print(f"Uploaded log to s3://{results_bucket}/{log_key}")
        else:
            print(f"Log file not found: {log_file}")

        # 3. Update the job item in the DynamoDB table
        complete_time = int(time.time())
        try:
            # Guard against regression: set COMPLETED only if currently RUNNING (or not set)
            # https://stackoverflow.com/questions/37053595/how-do-i-conditionally-insert-an-item-into-a-dynamodb-table-using-boto3
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
            table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET s3_results_bucket = :rb, "
                    "s3_key_result_file = :rk, "
                    "s3_key_log_file = :lk, "
                    "complete_time = :ct, "
                    "job_status = :done"
                ),
                ConditionExpression="job_status = :running",
                ExpressionAttributeValues={
                    ":rb": results_bucket,
                    ":rk": result_key,
                    ":lk": log_key,
                    ":ct": complete_time,
                    ":done": status_completed,
                    ":running": status_running
                }
            )
            print(f"Updated DynamoDB job {job_id} to {status_completed}")
            
            # 4. Publish notification to SNS topic
            # Only publish if DynamoDB update succeeded
            sns_results_topic = config.get("sns", "JobResultsTopic")
            topic_arn = get_topic_arn(sns_results_topic, region)
            
            if topic_arn:
                # Prepare the notification message with all job information
                notification_data = {
                    'job_id': job_id,
                    'user_id': user_id,
                    'user_email': user_email,
                    'input_file_name': base + '.vcf',
                    's3_results_bucket': results_bucket,
                    's3_key_result_file': result_key,
                    's3_key_log_file': log_key,
                    'complete_time': complete_time,
                    'job_status': status_completed
                }
                
                # Publish to SNS
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
                try:
                    sns = boto3.client('sns', region_name=region)
                    sns.publish(
                        TopicArn=topic_arn,
                        Message=json.dumps(notification_data),
                        Subject=f'Annotation job {job_id} completed'
                    )
                    print(f"Published completion notification for job {job_id} to SNS topic")
                except ClientError as e:
                    print(f"Error publishing to SNS: {e.response['Error']['Message']}")
                    # Don't fail the job if notification fails
                except Exception as e:
                    print(f"Error publishing to SNS: {str(e)}")
                    # Don't fail the job if notification fails
                
                # Also publish to archive topic for A14 archival
                try:
                    archive_topic_arn = get_topic_arn(archive_topic, region)
                    if archive_topic_arn:
                        sns.publish(
                            TopicArn=archive_topic_arn,
                            Message=json.dumps(notification_data),
                            Subject=f'Archive check for job {job_id}'
                        )
                        print(f"Published archive notification for job {job_id} to archive topic")
                except Exception as e:
                    print(f"Error publishing to archive topic: {str(e)}")
                    # Don't fail the job if archive notification fails
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                print(f"[WARN] Job {job_id} was not in RUNNING state; skipping update")
            else:
                print(f"ERROR: DynamoDB update failed: {e.response['Error']['Message']}")
        except Exception as e:
            print(f"ERROR: DynamoDB update failed: {e}")

        # 5. Clean up (delete) local job files
        # https://stackoverflow.com/questions/10873364/shutil-rmtree-clarification
        # https://www.geeksforgeeks.org/python/delete-an-entire-directory-tree-using-python-shutil-rmtree-method/
        shutil.rmtree(job_dir, ignore_errors=True)
        print("Cleaned up local job directory.")
    except Exception as e:
        print(f"Error uploading to S3 or cleaning up: {e}")


if __name__ == "__main__":
    main()

### EOF
