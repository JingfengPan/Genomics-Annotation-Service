# annotator_webhook.py
#
# Modified to run as a web server that can be called by SNS to process jobs
# Run using: python annotator_webhook.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import json
import os
import subprocess
import boto3
import requests
from botocore.exceptions import ClientError
from flask import Flask, jsonify, request

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration and add to Flask app object
environment = "annotator_webhook_config.Config"
app.config.from_object(environment)

# Connect to SQS and get the message queue
aws_region = app.config["AWS_REGION_NAME"]
queue_name = app.config["AWS_SQS_REQUESTS_QUEUE_NAME"]
queue_url = f"https://sqs.{aws_region}.amazonaws.com/127134666975/{queue_name}"

sqs_client = boto3.client("sqs", region_name=aws_region)
s3_client = boto3.client("s3", region_name=aws_region)
ddb_resource = boto3.resource("dynamodb", region_name=aws_region)
annotations_table = ddb_resource.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

os.makedirs(app.config["ANNOTATOR_JOBS_DIR"], exist_ok=True)


def parse_sns_envelope(body: str) -> dict:
    try:
        outer = json.loads(body)
    except json.JSONDecodeError:
        return {}
    message = outer.get("Message")
    if message is None:
        return outer
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        return {}


def confirm_subscription(payload: dict) -> bool:
    subscribe_url = payload.get("SubscribeURL")
    if not subscribe_url:
        app.logger.error("Missing SubscribeURL in confirmation payload.")
        return False
    try:
        response = requests.get(subscribe_url, timeout=5)
        # https://www.geeksforgeeks.org/python/response-raise_for_status-python-requests/
        response.raise_for_status()
        app.logger.info("SNS subscription confirmed.")
        return True
    except requests.RequestException as exc:
        app.logger.error(f"Failed to confirm subscription: {exc}")
        return False


def process_job_requests() -> int:
    max_messages = app.config["AWS_SQS_MAX_MESSAGES"]
    wait_time = app.config["AWS_SQS_WAIT_TIME"]
    visibility = app.config["AWS_SQS_VISIBILITY_TIMEOUT"]
    jobs_dir = app.config["ANNOTATOR_JOBS_DIR"]
    run_script = app.config["ANNOTATOR_RUN_SCRIPT"]
    status_running = app.config["JOB_STATUS_RUNNING"]
    status_pending = app.config["JOB_STATUS_PENDING"]

    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility,
        )
    except ClientError as exc:
        app.logger.error(
            "Failed to receive messages from queue %s: %s",
            queue_name,
            exc.response["Error"]["Message"],
        )
        return 0
    except Exception as exc:
        app.logger.exception("Unexpected failure receiving messages: %s", exc)
        return 0

    messages = response.get("Messages", [])
    if not messages:
        app.logger.info("Webhook triggered but queue is empty.")
        return 0

    processed = 0

    for message in messages:
        receipt_handle = message.get("ReceiptHandle")
        job_payload = parse_sns_envelope(message.get("Body", ""))

        if not job_payload:
            app.logger.warning("Received unparsable message; deleting from queue.")
            if receipt_handle:
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            continue

        job_id = job_payload.get("job_id")
        user_id = job_payload.get("user_id")
        bucket = job_payload.get("s3_inputs_bucket")
        key = job_payload.get("s3_key_input_file")
        filename = job_payload.get("input_file_name")

        if not all([job_id, user_id, bucket, key, filename]):
            app.logger.error("Message missing required fields; deleting.")
            if receipt_handle:
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            continue

        job_dir = os.path.join(jobs_dir, user_id, job_id)
        os.makedirs(job_dir, exist_ok=True)
        local_input = os.path.join(job_dir, filename)

        try:
            s3_client.download_file(bucket, key, local_input)
            app.logger.info("Downloaded input for job %s to %s", job_id, local_input)
        except Exception as exc:
            app.logger.error("Failed to download input for job %s: %s", job_id, exc)
            if receipt_handle:
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            continue

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        try:
            annotations_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET job_status = :running",
                ConditionExpression="job_status = :pending",
                ExpressionAttributeValues={
                    ":running": status_running,
                    ":pending": status_pending,
                },
            )
        except Exception as exc:
            app.logger.warning("Failed to update job %s status in DynamoDB: %s", job_id, exc)

        try:
            subprocess.Popen(["python3", run_script, local_input])
            app.logger.info("Launched annotation subprocess for job %s", job_id)
        except Exception as exc:
            app.logger.error("Failed to launch subprocess for job %s: %s", job_id, exc)
            if receipt_handle:
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            continue

        if receipt_handle:
            try:
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                app.logger.info("Deleted SQS message for job %s", job_id)
            except Exception as exc:
                app.logger.error("Failed to delete message for job %s: %s", job_id, exc)

        processed += 1

    return processed


@app.route("/", methods=["GET"])
def annotator_webhook():

    return ("Annotator webhook; POST job to /process-job-request"), 200


"""
A13 - Replace polling with webhook in annotator

Receives request from SNS; queries job queue and processes message.
Reads request messages from SQS and runs AnnTools as a subprocess.
Updates the annotations database with the status of the request.
"""


@app.route("/process-job-request", methods=["POST"])
def annotate():

    # https://tedboy.github.io/flask/generated/generated/flask.Request.get_json.html
    payload = request.get_json(force=True, silent=True)
    if payload is None:
        app.logger.error("Invalid or missing JSON payload.")
        return jsonify(
                    {
                        "code": 400, 
                        "message": "Invalid SNS payload."
                    }
                ), 400

    # https://tedboy.github.io/flask/interface_api.incoming_request_data.html#flask.Request.headers
    message_type = payload.get("Type") or request.headers.get("x-amz-sns-message-type")
    # Check message type
    if message_type == "SubscriptionConfirmation":
        # Confirm SNS topic subscription
        confirmed = confirm_subscription(payload)
        status_code = 200 if confirmed else 500
        message = "Subscription confirmed." if confirmed else "Subscription confirmation failed."
        return jsonify(
                    {
                        "code": status_code,
                        "message": message
                    }
                ), status_code

    if message_type != "Notification":
        app.logger.warning("Received unsupported SNS message type: %s", message_type)
        return jsonify(
                    {
                        "code": 400, 
                        "message": "Unsupported SNS message type."
                    }
                ), 400

    # Process job request
    processed = process_job_requests()

    if processed == 0:
        return jsonify(
                    {
                        "code": 202, 
                        "message": "No jobs available to process."
                    }
                ), 202

    return (
        jsonify(
            {
                "code": 201,
                "message": "Annotation job request processed."
            }
        ),
        201,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

### EOF
