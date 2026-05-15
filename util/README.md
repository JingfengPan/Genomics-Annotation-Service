# GAS Utilities
This directory contains the following utility-related files:
* `helpers.py` - Miscellaneous helper functions
* `util_config.ini` - Common configuration options for all utility scripts
* `ann_load.py` - Annotator load testing script (if you completed A20)

Each utility must be in its own sub-directory, along with its respective configuration file and run script, as follows:

/notify (for A12)
* `notify.py` - Sends notification email on completion of annotation job
* `notify_config.ini` - Configuration options for notification utility
* `run_notify.sh` - Runs the notifications utility script

/archive (for A14)
If using a script for the archival utility, you must include the following:
* `archive_scipt.py` - Archives free user result files to Glacier using a script
* `archive_script_config.ini` - Configuration options for archive utility script
* `run_archive_scipt.sh` - Runs the archive script

If you implemented the archival utility using a Flask app with a webhook, you must include the following:
* `archive_app.py` - Archives free user result files to Glacier using a Flask app
* `archive_app_config.py` - Configuration options for archive utility Flask app
* `run_archive_app.sh` - Runs the archive Flask app

The archive Flask app must listen on port 5001 (not 5000), as specified in `run_archive_app.sh`.

/thaw  (for A16)
* `thaw_script.py` - Thaws an archived Glacier object using a script
* `thaw_script_config.ini` - Configuration options for thaw utility script
* `run_thaw_scipt.sh` - Runs the thaw script

If you implemented the thawing utility using a Flask app with a webhook, you must include the following:
* `thaw_app.py` - Thaws an archived Glacier object using a Flask app
* `thaw_app_config.py` - Configuration options for thaw utility Flask app
* `run_thaw_app.sh` - Runs the thaw Flask app

The archive Flask app must listen on port 5002 (not 5000), as specified in `run_thaw_app.sh`.

/restore  (for A16)
* `restore.py` - The code for your AWS Lambda function that restores thawed objects to S3

In addition to the above, you must include any other code you used to implement the utility services in their respective directories.

## A14: Data Archival Implementation

### Approach (What and How)

The archive system moves free user result files from S3 to Glacier after a 3-minute grace period. Here's the flow:

1. When a job completes, the annotator publishes to SNS topic `jfpan01_a14_archive_topic` with job metadata.
2. Messages flow into SQS queue `jfpan01_a14_archive_queue`.
3. The archive script polls the queue. For each message, it checks elapsed time since `complete_time`.
4. If less than 180 seconds have passed, the message is requeued with SQS `DelaySeconds` (no blocking).
5. After 3 minutes, it checks the user's current role. Premium users are skipped.
6. For free users, it downloads from S3, uploads to Glacier, stores the archive ID in DynamoDB, and deletes from S3.
7. Messages are deleted after successful processing, or kept for retry on errors.

### Rationale (Why)

* **Script vs Flask App**: I chose a script-based approach instead of a Flask app because this is a long-running background service that just needs to poll a queue continuously. A Flask app would add unnecessary overhead (web server, routing, request handling) when all we need is a simple polling loop. The script is lighter, easier to deploy, and matches the pattern used for other utilities like the notification service.

* **Scalability**: Uses SQS `DelaySeconds` instead of blocking/sleeping. SQS handles delays server-side, so the script keeps processing other messages. No database polling or blocking operations.

* **Consistency**: Checks user role at archive time (not submission time), so users who upgrade during the grace period keep their files accessible.

* **Reliability**: Idempotency checks prevent duplicates. Failed operations keep messages in queue for retry. SNS-->SQS provides message durability.

The key is leveraging AWS services: SQS for queuing/delays, SNS for pub/sub, Glacier for storage. The script just orchestrates without heavy lifting.

## A16: Data Restoration Implementation

### Overview

The restoration system retrieves archived files from Glacier and restores them to S3 when free users upgrade to premium. This implements graceful degradation by attempting expedited retrieval first (a few minutes), falling back to standard retrieval (a few hours) if needed, ensuring both optimal user experience and guaranteed reliability.

### Approach (What and How)

The process is divided into two distinct stages:

**Stage 1: Thawing (thaw_script.py)**
1. When a user upgrades via `/subscribe`, the web app queries DynamoDB for all jobs with `results_file_archive_id`.
2. For each archived job, it publishes a message to SNS topic `jfpan01_a16_thaw_topic`.
3. Messages flow into SQS queue `jfpan01_a16_thaw_queue`.
4. The thaw script polls the queue and initiates Glacier retrieval jobs.
5. **Graceful Degradation**: Attempts Expedited retrieval first (a few minutes). If it fails with `InsufficientCapacityException`, falls back to Standard retrieval (a few hours).
6. Updates DynamoDB with `restore_job_id`, `restore_status`, `restore_tier`, and `restore_initiated_time`.
7. Messages are deleted only after BOTH Glacier retrieval initiation AND DynamoDB update succeed (transactional integrity).

**Stage 2: Restoration (restore.py Lambda)**
1. Triggered automatically by Glacier job completion notification via SNS topic `jfpan01_a16_glacier_restore`.
2. Parses Glacier notification to extract `restore_job_id` and queries DynamoDB to find the corresponding `job_id`.
3. Downloads the thawed archive from Glacier using `get_job_output()`.
4. Uploads the file back to S3 `gas-results` bucket with AES256 encryption.
5. Deletes the archive from Glacier to avoid storage costs.
6. Removes restoration-related fields from DynamoDB: `results_file_archive_id`, `restore_job_id`, `restore_status`, `restore_tier`, `restore_initiated_time`.
7. User can now download the file via the job details page.

### Rationale (Why)

**Two-Stage Design**:
- Glacier has an inherent two-phase process: initiate retrieval --> wait --> retrieve data. Our architecture maps directly to this.
- Separating thawing from restoration allows the thaw script to quickly process many requests without blocking on long-running Glacier operations.
- Lambda handles the restoration step because it's event-driven and only runs when needed (cost-efficient for sporadic completions).

**Script for Thawing vs Lambda for Restoration**:
- **Thaw Script**: Runs continuously on the utility instance to poll the queue and initiate Glacier jobs. This is a lightweight operation that just makes API calls, so a script is simpler than a Flask app. The script configures Glacier to send SNS notifications to `jfpan01_a16_glacier_restore` when retrieval completes.
- **Lambda for Restoration**: Glacier retrievals complete asynchronously after hours. Lambda is ideal because:
  - Event-driven: Only runs when Glacier sends completion notification via SNS (fully automatic)
  - No idle resources: Don't need a constantly-running service waiting for Glacier
  - Stateless: Each restoration is independent
  - Auto-scaling: Can handle multiple concurrent completions
  - No polling required: Glacier directly notifies Lambda through SNS subscription

**Graceful Degradation (Expedited --> Standard)**:
- Expedited retrieval provides a few minute restoration when capacity is available (better user experience).
- Standard retrieval takes a few hours but always succeeds (guaranteed reliability).
- Catching the specific `InsufficientCapacityException` error allows automatic fallback without user intervention.
- Clear console messages show which tier was used, helping with testing and debugging.

**Transactional Integrity in Thaw Script**:
- The script checks both Glacier initiation success AND DynamoDB update success before deleting the SQS message.
- If DynamoDB fails, the message stays in queue for retry, preventing orphaned Glacier jobs without database records.
- This ensures eventual consistency even under partial failures.

**State Management in DynamoDB**:
- Adding `restore_job_id`, `restore_tier`, `restore_status` allows tracking restoration progress.
- These fields are removed after successful restoration to keep the data model clean.
- We don't store user role in DynamoDB (per assignment requirements) - role is always fetched from the accounts database when needed.

**Complete Message Flow**:
The restoration process follows this sequence: 
1. User upgrades to premium via the web app, which queries DynamoDB for archived jobs and publishes a message to the thaw SNS topic for each one. 
2. The message is delivered to the thaw SQS queue, where the thaw script polls and receives it. 
3. The thaw script initiates a Glacier retrieval job (attempting Expedited first, falling back to Standard if needed) with `SNSTopic` parameter set to `jfpan01_a16_glacier_restore`, and updates DynamoDB with the restore job ID, tier, and status. 
4. Glacier processes the retrieval asynchronously over the next few minutes or hours depending on the tier. 
5. When Glacier completes the retrieval, it automatically sends a completion notification to SNS topic `jfpan01_a16_glacier_restore`. 
6. The Lambda function is subscribed to this SNS topic and is triggered automatically. It parses the Glacier notification, queries DynamoDB to find the job, downloads the thawed archive from Glacier, uploads it back to the S3 results bucket, deletes the archive from Glacier, and removes all restoration-related fields from DynamoDB. 
7. Finally, the user can access and download their restored file from the job details page.

**Glacier SNS Notification Architecture**:
- The thaw script specifies `SNSTopic` parameter when initiating Glacier retrievals, directing Glacier to send completion notifications to `jfpan01_a16_glacier_restore`.
- Lambda is subscribed to this SNS topic, creating a fully event-driven architecture.
- This eliminates the need for polling or monitoring loops: Glacier directly notifies Lambda when data is ready.
- More efficient than periodic status checks: zero unnecessary API calls, immediate response when jobs complete.
- Scalable: handles any number of concurrent restorations without additional infrastructure.

**Error Handling Philosophy**:
- **Thaw Script**: Malformed messages are deleted (permanent errors). Glacier or DynamoDB failures keep messages for retry (transient errors).
- **Lambda**: Returns proper HTTP status codes. Logs all errors to CloudWatch. Non-critical failures (like Glacier deletion) don't fail the entire restoration.
- Both services log extensively for debugging and monitoring.

**Cost Optimization**:
- Expedited retrievals cost more but complete faster - only used when available.
- Standard retrievals are cheaper - used as fallback.
- Lambda is pay-per-execution, only runs when Glacier notifies completion (no polling costs).
- Event-driven architecture eliminates unnecessary API calls and compute time.
- Archives are deleted from Glacier after restoration to avoid ongoing storage fees.
- SNS notifications are extremely low cost compared to polling alternatives.

The architecture leverages AWS-native services (SNS, SQS, Lambda, Glacier) for durability, scalability, and cost-efficiency, while maintaining clear separation of concerns between initiation and completion phases.