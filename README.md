# Genomics Annotation Service (GAS)

A cloud-based Software-as-a-Service platform for genomics annotation. GAS lets users authenticate through Globus Auth, upload VCF files securely to Amazon S3, submit annotation jobs, track job status, and retrieve completed results through a Flask web interface.

The system is organized around an event-driven AWS workflow: the web tier accepts user requests, annotation jobs are dispatched through SNS/SQS, worker scripts process jobs asynchronously, metadata is tracked in DynamoDB and PostgreSQL, and results are stored in S3 or archived to Glacier based on user tier.

> Built as the capstone for MPCS 51083 Cloud Computing at the University of Chicago.

---

## Architecture

Directory contents are as follows:

| Directory | Role |
|---|---|
| `/web` | Flask web application for authentication, job submission, job tracking, and result retrieval |
| `/ann` | Annotation workers that consume SQS messages and run AnnTools |
| `/util` | Utility scripts for notification, archival, restoration, and load generation |
| `/aws` | EC2 user-data bootstrap scripts for web, annotator, and utility instances |

<img width="663" height="566" alt="Architecture diagram" src="https://github.com/user-attachments/assets/b431ee3a-86e0-43d0-b8f5-6f9a422fe07e" />

<img width="651" height="709" alt="Data flow diagram" src="https://github.com/user-attachments/assets/7dd77259-4523-4013-a9f5-2b4b6b92fef6" />

### Component Breakdown

| Layer | Service / Tool | Role in the system |
|---|---|---|
| Web application | Flask + uWSGI | Serves the user interface, handles Globus Auth login, accepts VCF submissions, and exposes job/result pages |
| Compute | EC2 | Hosts the Flask web service, annotator workers, and utility scripts |
| Hot object storage | S3 | Stores uploaded VCF files, annotation results, and job logs |
| Cold archival storage | Glacier | Archives completed Free-tier results after the retention window |
| Job metadata | DynamoDB | Tracks job status, timestamps, S3 object keys, Glacier archive IDs, and restoration state |
| User database | RDS/PostgreSQL | Stores user profiles, subscription role, and account state through SQLAlchemy models |
| Messaging | SNS + SQS | Decouples job submission, annotation, archival, restoration, and notification workflows |
| Restoration automation | Lambda | Handles Glacier restoration completion and moves restored results back to S3 |
| Email notification | SES | Sends automated job-completion emails |
| Secrets | AWS Secrets Manager | Retrieves sensitive runtime configuration such as Flask, database, and Globus Auth secrets |
| Authentication | Globus Auth | Provides OAuth2-based federated login |

---

## Features

- **Secure VCF upload** — Uses presigned S3 POST policies so users upload files directly to S3 without exposing permanent AWS credentials.
- **Asynchronous annotation** — Submissions are published to SNS/SQS, allowing annotation workers to process jobs independently from the web request lifecycle.
- **Job tracking and result retrieval** — Stores job metadata in DynamoDB and exposes presigned S3 download URLs for completed results.
- **Tiered user access** — Maintains Free/Premium user roles through the PostgreSQL profile model and applies role-specific storage behavior.
- **Cost-optimized archival** — Archives Free-tier results to Glacier after a retention period while keeping Premium-user results available in S3.
- **Premium restoration workflow** — On upgrade, archived results can be restored from Glacier and made available again through S3.
- **Automated notifications** — Sends job-completion emails through AWS SES.
- **EC2 bootstrapping** — Includes user-data scripts for reproducible setup of web, annotator, and utility instances.

---

## Engineering Highlights

### Decoupled, Event-Driven Processing

The web service does not run expensive annotation work synchronously. Instead, it accepts a user submission, records job metadata, uploads the input file to S3, and publishes a message to SNS/SQS. Annotator workers consume queue messages and run AnnTools independently.

This design separates user-facing latency from backend compute time and allows the annotation layer to be scaled independently from the web tier.

### Secure Upload and Retrieval

File upload and result download are mediated through presigned S3 URLs. This keeps AWS credentials out of the browser while still allowing direct object transfer between the user and S3.

Uploaded inputs, result files, and logs are stored as S3 objects. Completed result files can be retrieved through generated download links from the job detail page.

### Cost-Optimized Storage Lifecycle

Free-tier result files are archived to Glacier after a grace period. The archive utility:

1. Receives completed-job messages from SQS.
2. Checks whether the user is still a Free-tier user.
3. Downloads the result file from S3.
4. Uploads the result to Glacier.
5. Records the Glacier archive ID in DynamoDB.
6. Removes the hot S3 copy to reduce storage cost.

Premium users are skipped during archival, so their results remain immediately accessible.

### Premium Upgrade and Glacier Restoration

When a user upgrades to Premium, the web application identifies archived jobs and publishes restoration requests. The restoration workflow is split into two stages:

- **Thaw script** — Initiates Glacier retrieval and records restore metadata in DynamoDB.
- **Restore Lambda** — Runs after Glacier retrieval completion, uploads the restored result back to S3, deletes the Glacier archive, and cleans up restoration fields in DynamoDB.

This avoids keeping a server idle while waiting for Glacier to complete retrieval.

### Runtime Configuration and Secret Management

Configuration is centralized across Flask and utility scripts:

- `web/config.py` defines Flask, AWS, database, S3, SNS/SQS, DynamoDB, Glacier, and Globus Auth settings.
- Utility scripts use `.ini` files such as `annotator_config.ini`, `archive_script_config.ini`, `thaw_script_config.ini`, and `util_config.ini`.
- AWS Secrets Manager is used for sensitive runtime values such as Flask, RDS/PostgreSQL, and Globus Auth secrets.

**Security note:** Before using or presenting the repository publicly, ensure that any test keys or local development secrets have been removed from source history and rotated if necessary.

### Deployment Bootstrap

The repository includes EC2 user-data scripts for provisioning web, annotator, and utility instances. These scripts install dependencies, fetch the project bundle, configure services, and start the relevant application or worker process.

Load balancers, Auto Scaling Groups, CloudWatch alarms, ACM certificates, and Terraform modules are not included as source-controlled infrastructure definitions in this repository. They should be treated as external AWS deployment configuration unless added to the repo later.

---

## Tech Stack

| Category | Technologies |
|---|---|
| Languages | Python 3, HTML/Jinja2, Bash |
| Web | Flask, uWSGI, Bootstrap |
| Authentication | Globus Auth OAuth2 |
| Data | PostgreSQL/RDS, SQLAlchemy, DynamoDB |
| Storage | S3, Glacier |
| Messaging | SNS, SQS |
| Serverless / Notifications | Lambda, SES |
| Security / Config | AWS Secrets Manager, presigned S3 URLs, HTTPS runtime configuration |
| Deployment support | EC2 user-data scripts |
| Load generation | Custom SQS load-generation script |

---

## Project Structure

```text
gas/
├── web/                        # Flask web application
│   ├── app.py                  # Flask app initialization
│   ├── views.py                # Routes and business logic
│   ├── config.py               # Runtime configuration and AWS clients
│   ├── auth.py                 # Globus Auth integration
│   ├── models.py               # SQLAlchemy user/profile models
│   ├── templates/              # Jinja2 templates
│   └── run_gas.sh              # uWSGI launcher with HTTPS configuration
├── ann/                        # Annotation worker service
│   ├── annotator.py            # SQS consumer that launches AnnTools jobs
│   ├── run.py                  # Annotation execution and post-processing
│   ├── annotator_config.ini    # Shared annotator configuration
│   ├── run_ann.sh              # Worker startup script
│   ├── annotator_webhook.py    # Optional Flask webhook variant
│   └── run_ann_webhook.sh      # Webhook startup script
├── util/
│   ├── helpers.py              # Shared helper functions, including SES email utility
│   ├── util_config.ini         # Common utility configuration
│   ├── ann_load.py             # Custom load-generation scaffold
│   ├── notify/                 # Job-completion notification utility
│   ├── archive/                # Free-tier archival to Glacier
│   ├── thaw/                   # Glacier retrieval initiator
│   └── restore/                # Lambda restoration handler
└── aws/                        # EC2 user-data bootstrap scripts
    ├── user_data_web_server.txt
    ├── user_data_annotator.txt
    └── user_data_utils.txt
```

---

## Component Reference

### `/web` — Web Application

The `/web` directory contains the user-facing Flask application. It handles:

- Globus Auth login and session management
- User profile and subscription state
- VCF upload initiation through presigned S3 POST
- Annotation job creation
- Job status pages
- Result download through presigned S3 GET URLs
- Premium subscription workflow and restoration trigger

Key files:

| File | Purpose |
|---|---|
| `app.py` | Flask app initialization |
| `views.py` | Routes for upload, annotation, job status, results, and subscription |
| `config.py` | Runtime configuration, AWS clients, bucket/table/topic names, and Secrets Manager access |
| `auth.py` | Globus Auth OAuth2 integration |
| `models.py` | SQLAlchemy models for user profile and subscription role |
| `run_gas.sh` | Starts the Flask application through uWSGI with HTTPS configuration |

### `/ann` — Annotator Workers

The `/ann` directory contains the annotation worker layer. Workers poll SQS for job messages, download input metadata, run AnnTools, upload outputs to S3, update DynamoDB, and publish completion events.

| File | Purpose |
|---|---|
| `annotator.py` | Long-running SQS consumer that starts annotation jobs |
| `run.py` | Executes AnnTools and performs completion-time updates |
| `annotator_config.ini` | Shared configuration for annotation scripts |
| `run_ann.sh` | Starts the annotator worker |

Optional webhook-based files are also present:

| File | Purpose |
|---|---|
| `annotator_webhook.py` | Flask webhook variant for annotation triggering |
| `annotator_webhook_config.py` | Configuration for webhook mode |
| `run_ann_webhook.sh` | Starts the webhook app |

### `/util/notify` — Job Completion Notifications

| File | Purpose |
|---|---|
| `notify.py` | Sends job-completion notification emails |
| `notify_config.ini` | Notification utility configuration |
| `run_notify.sh` | Starts the notification utility |

The notification workflow uses job-completion events and AWS SES to inform users when annotation results are available.

### `/util/archive` — Free-Tier Archival

| File | Purpose |
|---|---|
| `archive_script.py` | Archives Free-tier result files from S3 to Glacier |
| `archive_script_config.ini` | Archive utility configuration |
| `run_archive_script.sh` | Starts the archive utility |

Archival is intentionally delayed so users have a short retention window after job completion. The script checks user role at archive time, meaning a user who upgrades during the grace period can keep results available in S3.

### `/util/thaw` — Glacier Retrieval Initiation

| File | Purpose |
|---|---|
| `thaw_script.py` | Initiates Glacier retrieval jobs for archived results |
| `thaw_script_config.ini` | Thaw utility configuration |
| `run_thaw_script.sh` | Starts the thaw utility |

The thaw utility is triggered by Premium upgrade events and initiates Glacier restore jobs. It records restore status, restore job ID, retrieval tier, and initiation time in DynamoDB.

### `/util/restore` — Glacier to S3 Restoration

| File | Purpose |
|---|---|
| `restore.py` | Lambda handler that restores Glacier output back to S3 |

The restore Lambda handles Glacier completion notifications. It locates the corresponding job metadata, retrieves the archive, writes the result back to S3, deletes the Glacier archive, and updates DynamoDB so users can access restored results again.

### `/aws` — EC2 User Data

| File | Purpose |
|---|---|
| `user_data_web_server.txt` | Bootstraps web server instances |
| `user_data_annotator.txt` | Bootstraps annotation worker instances |
| `user_data_utils.txt` | Bootstraps utility instances and starts background utilities |

These scripts encode deployment steps so instances can be recreated without manual SSH configuration.

---

## Scalability Notes

The repository demonstrates a scalable design pattern by separating the web tier, worker tier, storage layer, metadata layer, and notification/restoration utilities.

Important scalability properties:

- Web requests are kept lightweight because annotation work is delegated to queue-backed workers.
- SQS buffers job spikes and prevents transient load from overwhelming annotator instances.
- SNS topics allow multiple downstream workflows, such as annotation completion, archival, restoration, and notification, to be added without tightly coupling components.
- S3 and Glacier separate hot result retrieval from low-cost long-term archival.
- DynamoDB provides a low-latency job metadata store suitable for frequent status updates.

AWS-managed load balancing, Auto Scaling, and CloudWatch monitoring can be added around this architecture in a deployment environment, but this repository currently focuses on the application code, worker code, utility workflows, and EC2 bootstrap scripts.

---

## License

Academic project. Not for commercial use.
