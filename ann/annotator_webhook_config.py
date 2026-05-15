# ann_config.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Set GAS annotator configuration options
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import os

# Get the IAM username that was stashed at launch time
try:
    with open("/home/ubuntu/.launch_user", "r") as file:
        iam_username = file.read().replace("\n", "")
except FileNotFoundError as e:
    if "LAUNCH_USER" in os.environ:
        iam_username = os.environ["LAUNCH_USER"]
    else:
        # Unable to set username, so exit
        print("Unable to find launch user name in local file or environment!")
        raise e


class Config(object):

    CSRF_ENABLED = True

    ANNOTATOR_BASE_DIR = "/home/ubuntu/gas/ann"
    ANNOTATOR_JOBS_DIR = f"{ANNOTATOR_BASE_DIR}/jobs"
    ANNOTATOR_RUN_SCRIPT = f"{ANNOTATOR_BASE_DIR}/run.py"

    AWS_REGION_NAME = (
        os.environ["AWS_REGION_NAME"]
        if ("AWS_REGION_NAME" in os.environ)
        else "us-east-1"
    )

    # AWS S3 upload parameters
    AWS_S3_INPUTS_BUCKET = "gas-inputs"
    AWS_S3_RESULTS_BUCKET = "gas-results"

    # AWS SNS topics

    # AWS SQS queues
    AWS_SQS_WAIT_TIME = 20
    AWS_SQS_MAX_MESSAGES = 10
    AWS_SQS_VISIBILITY_TIMEOUT = 60
    AWS_SQS_REQUESTS_QUEUE_NAME = f"{iam_username}_a14_job_requests"

    # AWS DynamoDB
    AWS_DYNAMODB_ANNOTATIONS_TABLE = f"{iam_username}_annotations"

    # Job status values
    JOB_STATUS_PENDING = "PENDING"
    JOB_STATUS_RUNNING = "RUNNING"
    JOB_STATUS_COMPLETED = "COMPLETED"
    JOB_STATUS_ERROR = "ERROR"


### EOF
