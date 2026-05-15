# notify.py
#
# Notify user of job completion via email
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import sys
from datetime import datetime

from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("notify_config.ini")

"""A12
Reads result messages from SQS and sends notification emails.
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


def handle_results_queue(sqs=None):

    # Get configuration values
    region = config['aws']['AwsRegionName']
    queue_name = config['sqs']['JobResultsQueueName']
    queue_url = f"https://sqs.{region}.amazonaws.com/127134666975/{queue_name}"
    wait_time = int(config['sqs']['WaitTime'])
    max_messages = int(config['sqs']['MaxMessages'])
    web_server_url = config['gas']['WebServerUrl']
    sender_email = config['gas']['MailDefaultSender']

    # Read messages from the queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time
        )
    except ClientError as e:
        print(f"[ERROR] Failed to receive messages: {e.response['Error']['Message']}")
        return
    except Exception as e:
        print(f"[ERROR] Generic receive failure: {e}")
        return

    messages = response.get("Messages", [])
    if not messages:
        # No messages received, continue polling
        return

    # Process messages --> send email to user
    for msg in messages:
        receipt_handle = msg["ReceiptHandle"]
        raw_body = msg.get("Body", "")
        job_data = parse_sns_envelope(raw_body)

        if not job_data:
            print("[WARN] Unparsable message received; deleting.")
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            except Exception as e:
                print(f"[ERROR] Failed to delete unparsable message: {e}")
            continue

        # Extract job parameters from message
        job_id = job_data.get("job_id")
        user_email = job_data.get("user_email")
        complete_time = job_data.get("complete_time")

        if not job_id or not user_email:
            print(f"[WARN] Missing job_id or user_email in message; deleting.")
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            except Exception as e:
                print(f"[ERROR] Failed to delete invalid message: {e}")
            continue

        print(f"[INFO] Processing notification for job {job_id}, sending to {user_email}")

        # Convert complete_time from epoch to human-readable format
        try:
            complete_datetime = datetime.fromtimestamp(int(complete_time))
            complete_time_formatted = complete_datetime.strftime('%Y-%m-%d @ %H:%M:%S')
        except (ValueError, TypeError) as e:
            print(f"[ERROR] Failed to format complete_time: {e}")
            complete_time_formatted = str(complete_time)

        # Construct the job details URL
        job_details_url = f"{web_server_url}/annotations/{job_id}"

        # Prepare email content
        subject = f"Results available for job {job_id}"
        body = (
            f"Your annotation job completed at {complete_time_formatted}.\n\n"
            f"Click here to view job details and results: {job_details_url}"
        )

        # Send email using SES
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ses/client/send_email.html
        try:
            helpers.send_email_ses(
                recipients=user_email,
                sender=sender_email,
                subject=subject,
                body=body
            )
            print(f"[INFO] Email sent to {user_email} for job {job_id}")
        except ClientError as e:
            print(f"[ERROR] Failed to send email: {e.response['Error']['Message']}")
            # Don't delete message if email fails - allow retry
            continue
        except Exception as e:
            print(f"[ERROR] Failed to send email: {str(e)}")
            # Don't delete message if email fails - allow retry
            continue

        # Delete messages
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
        try:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            print(f"[INFO] Deleted SQS message for job {job_id}")
        except Exception as e:
            print(f"[ERROR] Failed to delete message for job {job_id}: {e}")


def main():
    region = config['aws']['AwsRegionName']
    sqs = boto3.client("sqs", region_name=region)

    print(f"[INFO] Notification service started. Polling queue...")

    # Poll queue for new results and process them
    while True:
        handle_results_queue(sqs=sqs)


if __name__ == "__main__":
    main()

### EOF
