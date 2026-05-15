# annotator.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2013-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import subprocess
from botocore.exceptions import ClientError

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")


"""Reads request messages from SQS and runs AnnTools as a subprocess.

Move existing annotator code here
"""


# Helper function to parse SNS envelope from SQS message
# https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html
def parse_sns_envelope(body: str) -> dict:
    try:
        outer = json.loads(body)
    except json.JSONDecodeError:
        return {}
    if "Message" in outer:
        try:
            return json.loads(outer["Message"])
        except json.JSONDecodeError:
            return {}
    return outer


def handle_requests_queue(sqs=None, s3=None, ddb=None, table=None):

    # Get configuration values
    region = config.get("aws", "AwsRegionName")
    queue_name = config.get("sqs", "JobRequestsQueueName")
    queue_url = f"https://sqs.{region}.amazonaws.com/127134666975/{queue_name}"
    max_messages = config.getint("sqs", "MaxMessages")
    wait_time = config.getint("sqs", "WaitTime")
    visibility_timeout = config.getint("sqs", "VisibilityTimeout")
    jobs_dir = os.path.expanduser(config.get("ann", "JobsDirectory"))
    run_script = os.path.expanduser(config.get("ann", "RunScript"))
    status_running = config.get("job_status", "Running")
    status_pending = config.get("job_status", "Pending")

    # Attempt to read the maximum number of messages from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    try:
        # Process as many messages as are available
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout
        )
    except ClientError as e:
        print(f"[ERROR] Failed to receive messages: {e.response['Error']['Message']}")
        return
    except Exception as e:
        print(f"[ERROR] Generic receive failure: {e}")
        return

    messages = resp.get("Messages", [])
    if not messages:
        # no messages received, continue polling
        return

    # Process messages received
    # https://realbigdeo.medium.com/raw-messaging-delivery-in-aws-sns-sqs-subscriptions-2b683d657b01
    for msg in messages:
        receipt_handle = msg["ReceiptHandle"]
        raw_body = msg.get("Body", "")
        job = parse_sns_envelope(raw_body)

        if not job:
            print("[WARN] Unparsable message received; deleting.")
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            except Exception as e:
                print(f"[ERROR] Failed to delete unparsable message: {e}")
            continue

        # Extract job parameters from message body
        job_id = job["job_id"]
        user_id = job["user_id"]
        bucket = job["s3_inputs_bucket"]
        key = job["s3_key_input_file"]
        filename = job["input_file_name"]

        print(f"[INFO] Processing job {job_id} (input={filename})")

        # Include below the same code you used in prior homework
        # Get the input file S3 object and copy it to a local file
        # Use a local directory structure that makes it easy to organize
        # Local job directory: jobs/user_id/job_id
        job_dir = os.path.join(jobs_dir, user_id, job_id)
        os.makedirs(job_dir, exist_ok=True)
        local_input = os.path.join(job_dir, filename)

        # Download the input file from S3
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
        # https://stackoverflow.com/questions/29378763/how-to-save-s3-object-to-a-file-using-boto3
        try:
            s3.download_file(bucket, key, local_input)
        except Exception as e:
            print(f"[ERROR] Failed to download input from S3 for job {job_id}: {e}")
            # Delete the message to prevent it from being reprocessed infinitely
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            except Exception as de:
                print(f"[ERROR] Failed to delete message after download failure: {de}")
            continue

        # Update job status to RUNNING in DynamoDB
        try:
            table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET job_status = :running",
                ConditionExpression="job_status = :pending",
                ExpressionAttributeValues={
                    ":running": status_running,
                    ":pending": status_pending
                }
            )
        except Exception as e:
            print(f"[WARN] Failed to update DynamoDB for job {job_id}: {e}")

        # Launch annotation job as a background process
        try:
            ann_process = subprocess.Popen(
                ["python3", run_script, local_input]
            )
            print(f"[INFO] Spawned annotation process PID={ann_process.pid} for job {job_id}")
        except Exception as e:
            print(f"[ERROR] Failed to start annotation subprocess for job {job_id}: {e}")
            # Delete message even if subprocess fails to avoid endless retries
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            except Exception as de:
                print(f"[ERROR] Failed to delete message after spawn error: {de}")
            continue

        # Delete message from queue, if job was successfully submitted
        try:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            print(f"[INFO] Deleted SQS message for job {job_id}")
        except Exception as e:
            print(f"[ERROR] Failed to delete message for job {job_id}: {e}")


def main():

    # Get configuration values
    region = config.get("aws", "AwsRegionName")
    ddb_table = config.get("gas", "AnnotationsTable")
    jobs_dir = os.path.expanduser(config.get("ann", "JobsDirectory"))

    # Create jobs directory if it doesn't exist
    os.makedirs(jobs_dir, exist_ok=True)

    # Get handles to AWS services
    sqs = boto3.client("sqs", region_name=region)
    s3 = boto3.client("s3", region_name=region)
    ddb = boto3.resource("dynamodb", region_name=region)
    table = ddb.Table(ddb_table)

    print(f"[INFO] Annotator started. Polling queue...")

    # Poll queue for new results and process them
    while True:
        handle_requests_queue(sqs=sqs, s3=s3, ddb=ddb, table=table)


if __name__ == "__main__":
    main()

### EOF
