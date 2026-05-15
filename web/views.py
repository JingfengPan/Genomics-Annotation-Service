# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for, jsonify

from app import app, db
from decorators import authenticated, is_premium

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():

    region = app.config["AWS_REGION_NAME"]

    # Parse redirect URL query parameters for S3 object info
    bucket_name = request.args.get("bucket")
    s3_key = request.args.get("key")

    if not bucket_name or not s3_key:
        app.logger.error("Missing bucket or key in S3 redirect")
        return abort(400)

    # Get the authenticated user's ID and email from session
    user_id = session.get("primary_identity")
    user_email = session.get("email", "")
    
    if not user_id:
        app.logger.error("User ID not found in session")
        return abort(403)

    # Extract the job ID from the S3 key
    # Key format: cnetid/user_id/job_id~filename
    try:
        key_parts = s3_key.split("/")
        job_id_with_filename = key_parts[-1]
        job_id = job_id_with_filename.split("~")[0]
        input_file_name = job_id_with_filename.split("~")[-1]
    except (IndexError, ValueError) as e:
        app.logger.error(f"Failed to parse S3 key {s3_key}: {e}")
        return abort(400)

    submit_time = int(time.time())

    # Create a job item and persist it to the annotations database
    job_item = {
        "job_id": job_id,
        "user_id": user_id,
        "user_email": user_email,
        "input_file_name": input_file_name,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time": submit_time,
        "job_status": app.config["JOB_STATUS_PENDING"]
    }

    # Persist job to database
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/programming-with-python.html
    try:
        ddb = boto3.resource("dynamodb", region_name=region)
        table = ddb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        table.put_item(Item=job_item)
    except ClientError as e:
        app.logger.error(f"Failed to persist job to DynamoDB: {e}")
        return abort(500)

    # Send message to request queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    try:
        sns = boto3.client("sns", region_name=region)
        sns.publish(
            TopicArn=app.config["AWS_SNS_JOB_REQUEST_TOPIC"],
            Message=json.dumps(job_item),
            Subject=app.config["AWS_SNS_JOB_REQUEST_SUBJECT"]
        )
    except ClientError as e:
        app.logger.error(f"Failed to publish job to SNS: {e}")
        return abort(500)

    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():
    # Get list of annotations to display
    # Get the authenticated user's ID from the session
    user_id = session.get('primary_identity')
  
    if not user_id:
        app.logger.error("No user ID found in session")
        return abort(403)
    
    try:
        # Initialize DynamoDB resource
        dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        
        # Query DynamoDB for all jobs belonging to this user
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#boto3.dynamodb.conditions.Key
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
        response = table.query(
            IndexName=app.config['AWS_DYNAMODB_USER_ID_INDEX'], 
            KeyConditionExpression=Key('user_id').eq(user_id),
            # Sort by submit time in descending order
            # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ScanIndexForward
            ScanIndexForward=False
        )
        
        # Extract items from response
        # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_ResponseSyntax
        annotations = response.get('Items', [])
        
        for annotation in annotations:
            if 'submit_time' in annotation:
                # Convert epoch to datetime using server's local timezone
                # DynamoDB returns numbers as Decimal, must convert to int
                # https://docs.python.org/3/library/datetime.html#datetime.datetime.fromtimestamp
                submit_datetime = datetime.fromtimestamp(int(annotation['submit_time']))
                # Format as readable string
                # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
                annotation['submit_time_formatted'] = submit_datetime.strftime('%Y-%m-%d @ %H:%M:%S')

        app.logger.info(f"Retrieved {len(annotations)} annotations for user {user_id}")

        # Return the template with annotations
        return render_template('annotations.html', annotations=annotations)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
        if error_code == 'ResourceNotFoundException':
            app.logger.error(f"DynamoDB table not found: {app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']}")
        elif error_code == 'ValidationException':
            # This might occur if the GSI doesn't exist
            # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-IndexName
            app.logger.error(f"Query validation error: {error_message}")
        elif error_code == 'ProvisionedThroughputExceededException':
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughputExceeded.html
            app.logger.error(f"DynamoDB throughput exceeded: {error_message}")
        else:
            app.logger.error(f"DynamoDB error: {error_code} - {error_message}")
        
        # Return empty list on error
        return render_template('annotations.html', annotations=[])
        
    except Exception as e:
        # Catch any unexpected errors
        app.logger.error(f"Unexpected error retrieving annotations: {str(e)}")
        return render_template('annotations.html', annotations=[])


"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):
    # Get the authenticated user's ID from the session
    user_id = session.get('primary_identity')
    
    if not user_id:
        app.logger.error("No user ID found in session")
        return abort(403)
    
    # Initialize DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    
    # Get the annotation job from DynamoDB
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
    try:
        response = table.get_item(
            Key={'job_id': id}
        )
    except ClientError as e:
        app.logger.error(f"DynamoDB error: {e}")
        return abort(500)
    
    # Check if item exists
    if 'Item' not in response:
        app.logger.error(f"Job {id} not found")
        return abort(404)
    
    annotation = response['Item']
    
    # Verify the job belongs to the authenticated user
    if annotation.get('user_id') != user_id:
        app.logger.error(f"User {user_id} not authorized to view job {id}")
        return abort(403)
    
    # Convert submit_time
    if 'submit_time' in annotation:
        submit_datetime = datetime.fromtimestamp(int(annotation['submit_time']))
        annotation['submit_time_formatted'] = submit_datetime.strftime('%Y-%m-%d @ %H:%M:%S')
    
    # Convert complete_time if job is completed
    if 'complete_time' in annotation and annotation.get('job_status') == app.config['JOB_STATUS_COMPLETED']:
        complete_time_epoch = int(annotation['complete_time'])
        complete_datetime = datetime.fromtimestamp(complete_time_epoch)
        annotation['complete_time_formatted'] = complete_datetime.strftime('%Y-%m-%d @ %H:%M:%S')
    
    # Generate presigned URLs for file downloads
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    s3 = boto3.client('s3',
        region_name=app.config['AWS_REGION_NAME'],
        config=Config(signature_version='s3v4'))
    
    # Generate presigned URL for input file download
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
    try:
        input_file_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': annotation.get('s3_inputs_bucket', app.config['AWS_S3_INPUTS_BUCKET']),
                'Key': annotation['s3_key_input_file']
            },
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
        )
        annotation['input_file_url'] = input_file_url
    except ClientError as e:
        app.logger.error(f"Error generating presigned URL for input file: {e}")
        annotation['input_file_url'] = "#"
        
    # If job is completed, check if results are available
    if annotation.get('job_status') == app.config['JOB_STATUS_COMPLETED']:
        # Check if user is free and if free access period has expired
        user_role = session.get('role', 'free_user')
        free_access_expired = False
        
        if user_role == 'free_user':
            # Check if file is archived - if so, access has expired
            if 'results_file_archive_id' in annotation:
                free_access_expired = True
                app.logger.info(f"Free access expired for job {id} (file archived)")
            elif 'complete_time' in response['Item']:
                # Check if more than 3 minutes have passed since completion
                current_time = int(time.time())
                complete_time_epoch = int(annotation['complete_time'])
                elapsed_time = current_time - complete_time_epoch
                if elapsed_time > 180:
                    free_access_expired = True
                    app.logger.info(f"Free access expired for job {id} (elapsed: {elapsed_time}s)")

        # If not expired or user is premium, generate presigned URL for results
        if not free_access_expired and 's3_key_result_file' in annotation:
            if annotation.get('results_file_archive_id') and user_role == 'premium_user':
                # File was archived but user is now premium
                # Check if restoration is in progress
                if annotation.get('restore_job_id'):
                    restore_status = annotation.get('restore_status', 'PENDING')
                    restore_tier = annotation.get('restore_tier', 'Standard')
                    
                    if restore_status == 'PENDING':
                        # Restoration is in progress - show appropriate message
                        if restore_tier == 'Expedited':
                            annotation['restore_message'] = "File is being restored; your data will be available in a few minutes."
                        else:
                            annotation['restore_message'] = "File is being restored; your data will be available in a few hours."
                    elif restore_status == 'FAILED':
                        annotation['restore_message'] = "File restoration failed. Please contact support."
                else:
                    # File is archived but no restoration initiated yet
                    annotation['restore_message'] = "Results file is archived. Restoration will begin shortly."
            else:
                # File is not archived - generate normal download URL
                try:
                    result_file_url = s3.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                            'Key': annotation['s3_key_result_file']
                        },
                        ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
                    )
                    annotation['result_file_url'] = result_file_url
                except ClientError as e:
                    app.logger.error(f"Error generating presigned URL for results file: {e}")
                    # Check if the error is because file doesn't exist in S3
                    if e.response['Error']['Code'] == 'NoSuchKey':
                        annotation['restore_message'] = "Results file not found. It may have been archived."

        # Pass the free_access_expired flag to template
        return render_template('annotation.html', 
                              annotation=annotation, 
                              free_access_expired=free_access_expired)
    
    # For running jobs or jobs without free access issues
    return render_template('annotation.html', annotation=annotation)


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    # Get the authenticated user's ID from the session
    user_id = session.get('primary_identity')
    
    if not user_id:
        app.logger.error("No user ID found in session")
        return abort(403)
    
    # Initialize DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    
    # Get the annotation job from DynamoDB
    try:
        response = table.get_item(
            Key={'job_id': id}
        )
    except ClientError as e:
        app.logger.error(f"DynamoDB error: {e}")
        return abort(500)
    
    # Check if item exists
    if 'Item' not in response:
        app.logger.error(f"Job {id} not found")
        return abort(404)
    
    annotation = response['Item']
    
    # Verify the job belongs to the authenticated user
    if annotation.get('user_id') != user_id:
        app.logger.error(f"User {user_id} not authorized to view log for job {id}")
        return abort(403)
    
    # Check if job is completed and log file exists
    if annotation.get('job_status') != app.config['JOB_STATUS_COMPLETED']:
        app.logger.error(f"Job {id} is not completed yet")
        return abort(400)
    
    # Check if log file key exists
    if 's3_key_log_file' not in annotation:
        app.logger.error(f"No log file key found for job {id}")
        return abort(404)
    
    # Initialize S3 client
    s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
    
    try:
        # Get the log file from S3
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        log_response = s3.get_object(
            Bucket=app.config['AWS_S3_RESULTS_BUCKET'],
            Key=annotation['s3_key_log_file']
        )
        
        # Read the log file contents
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#streamingbody
        log_file_contents = log_response['Body'].read().decode('utf-8')
        
        app.logger.info(f"Successfully retrieved log file for job {id}")
        
        # Render the log view template
        return render_template('view_log.html',
            job_id=id,
            log_file_contents=log_file_contents
        )
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'NoSuchKey':
            app.logger.error(f"Log file not found in S3 for job {id}: {annotation['s3_key_log_file']}")
            return abort(404)
        elif error_code == 'AccessDenied':
            app.logger.error(f"Access denied to log file for job {id}: {error_message}")
            return abort(403)
        else:
            app.logger.error(f"S3 error retrieving log file: {error_code} - {error_message}")
            return abort(500)
            
    except UnicodeDecodeError as e:
        # https://docs.python.org/3/library/exceptions.html#UnicodeDecodeError
        app.logger.error(f"Error decoding log file for job {id}: {str(e)}")
        return abort(500)


"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
@authenticated
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info
        return render_template("subscribe.html")

    elif request.method == "POST":
        # Process the subscription request
        
        # Extract the Stripe token from the form
        stripe_token = request.form.get("stripe_token")
        
        if not stripe_token:
            app.logger.error("No Stripe token received")
            return abort(400)
        
        # Get user information from session
        user_id = session.get("primary_identity")
        user_email = session.get("email")
        user_name = session.get("name")
        
        if not user_id or not user_email:
            app.logger.error("Missing user information in session")
            return abort(403)
        
        # Set Stripe API key
        stripe.api_key = app.config["STRIPE_SECRET_KEY"]
        
        try:
            # Create a customer on Stripe
            # https://stripe.com/docs/api/customers/create
            customer = stripe.Customer.create(
                card=stripe_token,
                email=user_email,
                name=user_name
            )
            
            app.logger.info(f"Created Stripe customer {customer.id} for user {user_id}")
            
            # Subscribe customer to pricing plan
            # https://stripe.com/docs/api/subscriptions/create
            subscription = stripe.Subscription.create(
                customer=customer.id,
                items=[{"price": app.config["STRIPE_PRICE_ID"]}]
            )
            
            app.logger.info(f"Created subscription {subscription.id} for customer {customer.id}")
            
        except stripe.error.StripeError as e:
            app.logger.error(f"Stripe error: {e}")
            return abort(500)
        
        # Update user role in accounts database
        update_profile(identity_id=user_id, role="premium_user")
        app.logger.info(f"Updated user {user_id} role to premium_user in database")
        
        # Update role in the session
        session["role"] = "premium_user"
        app.logger.info(f"Updated user {user_id} role to premium_user in session")
        
        # Cancel any pending archivals for this user
        # Get messages from the archive queue and delete those belonging to this user
        try:
            sqs = boto3.client("sqs", region_name=app.config["AWS_REGION_NAME"])
            
            # Get the queue URL
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/get_queue_url.html
            queue_name = f"{app.config['AWS_S3_KEY_PREFIX'].rstrip('/')}_a16_archive_queue"
            
            try:
                queue_url_response = sqs.get_queue_url(QueueName=queue_name)
                queue_url = queue_url_response["QueueUrl"]
            except ClientError as e:
                if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                    app.logger.warning(f"Archive queue does not exist: {queue_name}")
                    # Queue doesn't exist yet, nothing to cancel
                else:
                    raise
            else:
                # Receive messages from the queue to check for this user's jobs
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
                cancelled_count = 0
                checked_count = 0
                
                while True:
                    messages_response = sqs.receive_message(
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=0,
                        VisibilityTimeout=10
                    )
                    
                    messages = messages_response.get("Messages", [])
                    
                    if not messages:
                        # No more messages in the queue
                        break
                    
                    # Check each message to see if it belongs to this user
                    for message in messages:
                        checked_count += 1
                        try:
                            message_body = json.loads(message["Body"])
                            # SNS wraps the message in a "Message" field
                            if "Message" in message_body:
                                job_data = json.loads(message_body["Message"])
                            else:
                                job_data = message_body
                            
                            # Check if this job belongs to the user who just subscribed
                            if job_data.get("user_id") == user_id:
                                # Delete this message from the queue
                                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
                                sqs.delete_message(
                                    QueueUrl=queue_url,
                                    ReceiptHandle=message["ReceiptHandle"]
                                )
                                cancelled_count += 1
                                app.logger.info(f"Cancelled pending archival for job {job_data.get('job_id')}")
                            else:
                                # Return message to queue by changing visibility timeout to 0
                                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/change_message_visibility.html
                                sqs.change_message_visibility(
                                    QueueUrl=queue_url,
                                    ReceiptHandle=message["ReceiptHandle"],
                                    VisibilityTimeout=0
                                )
                        except (json.JSONDecodeError, KeyError) as e:
                            app.logger.error(f"Error parsing archive queue message: {e}")
                            # Make message visible again by changing visibility timeout to 0
                            try:
                                sqs.change_message_visibility(
                                    QueueUrl=queue_url,
                                    ReceiptHandle=message["ReceiptHandle"],
                                    VisibilityTimeout=0
                                )
                            except ClientError:
                                pass  # Message may have already timed out
                
                app.logger.info(f"Checked {checked_count} messages in archive queue, cancelled {cancelled_count} for user {user_id}")
            
        except ClientError as e:
            # Log the error but don't fail the subscription
            app.logger.error(f"Error accessing archive queue: {e}")
        except Exception as e:
            app.logger.error(f"Unexpected error handling archive queue: {e}")
        
        # Request restoration of the user's data from Glacier
        # Query DynamoDB for all archived jobs belonging to this user
        try:
            dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
            table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
            sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
            
            # Query for all user's jobs that have been archived
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#ref-valid-filter-conditions
            response = table.query(
                IndexName=app.config['AWS_DYNAMODB_USER_ID_INDEX'],
                KeyConditionExpression=Key('user_id').eq(user_id),
                FilterExpression=Attr('results_file_archive_id').exists()
            )
            
            archived_jobs = response.get('Items', [])
            app.logger.info(f"Found {len(archived_jobs)} archived jobs for user {user_id}")
            
            if archived_jobs:
                # Get the thaw/restoration topic ARN
                # This topic triggers the thaw utility to initiate Glacier retrieval
                thaw_topic_arn = app.config['AWS_SNS_THAW_TOPIC']
                
                # Send thaw requests for each archived job
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
                for job in archived_jobs:
                    thaw_message = {
                        'job_id': job['job_id'],
                        'user_id': user_id,
                        'results_file_archive_id': job['results_file_archive_id'],
                        's3_key_result_file': job.get('s3_key_result_file', ''),
                        's3_results_bucket': app.config['AWS_S3_RESULTS_BUCKET']
                    }
                    
                    # Publish thaw request to SNS topic
                    sns.publish(
                        TopicArn=thaw_topic_arn,
                        Message=json.dumps(thaw_message),
                        Subject=f'Thaw request for job {job["job_id"]}'
                    )
                    
                    app.logger.info(f"Initiated thaw request for archived job {job['job_id']}")
                
                app.logger.info(f"Successfully initiated restoration for {len(archived_jobs)} archived jobs")
            else:
                app.logger.info(f"No archived jobs found for user {user_id}")
            
        except ClientError as e:
            # Log the error but don't fail the subscription
            app.logger.error(f"Error initiating restoration: {e}")
        except Exception as e:
            app.logger.error(f"Unexpected error during restoration initiation: {str(e)}")

        # Display confirmation page
        return render_template("subscribe_confirm.html", stripe_id=customer.id)


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
