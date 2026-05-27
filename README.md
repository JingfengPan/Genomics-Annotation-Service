# Genomics Annotation Service (GAS)

A scalable, fault-tolerant Software-as-a-Service platform for genomics annotation, built on AWS. GAS lets researchers upload VCF files, run annotation jobs, and retrieve results through a secure web interface — with automatic scaling, tiered subscriptions, event-driven processing, and cost-optimized long-term storage.

> Built as the capstone for MPCS 51083 Cloud Computing at the University of Chicago.

---

## Architecture

Directory contents are as follows:

| Directory | Role |
|---|---|
| `/web` | Flask web application (user-facing tier) |
| `/ann` | Annotator workers (SQS consumers → AnnTools) |
| `/util` | Utility scripts for notifications, archival, and restoration |
| `/aws` | EC2 user-data bootstrap scripts |

<img width="663" height="566" alt="Architecture diagram" src="https://github.com/user-attachments/assets/b431ee3a-86e0-43d0-b8f5-6f9a422fe07e" />

<img width="651" height="709" alt="Data flow diagram" src="https://github.com/user-attachments/assets/7dd77259-4523-4013-a9f5-2b4b6b92fef6" />

### Component breakdown

| Layer | AWS Service | Role |
|---|---|---|
| Web tier | EC2 + ELB + Auto Scaling | Stateless Flask/Gunicorn nodes behind an Application Load Balancer |
| Annotator farm | EC2 + Auto Scaling | Workers that consume jobs from SQS and run AnnTools |
| Hot object store | S3 | Input files, result files, job logs |
| Cold object store | Glacier | Archival tier for Free-user data |
| Job metadata | DynamoDB | Per-job status, timestamps, Glacier archive IDs |
| User accounts | RDS (PostgreSQL) | Profile + subscription state, accessed via SQLAlchemy/Alembic |
| Messaging | SNS + SQS | Decouples submission, processing, archival, and notification |
| Notifications | Lambda + SES | Serverless email on job completion |
| Secrets | AWS Secrets Manager | DB credentials with rotation |
| AuthN/AuthZ | Globus Auth (OAuth2) | Federated login (e.g., UChicago CNetID) |

---

## Features

- **Tiered subscriptions** — Free users capped on job size with 5-minute result retention; Premium users unlimited.
- **End-to-end HTTPS** — Enforced across the app; TLS termination at the ELB via ACM.
- **Event-driven pipeline** — Submission → SNS → SQS → Annotator → SNS → Lambda → SES. No synchronous blocking between tiers.
- **Graceful degradation** — Glacier restore first attempts Expedited retrieval, falls back to Standard on capacity failure.
- **Elastic compute** — Web tier scales on ELB request rate; annotator tier scales on SQS queue depth.
- **Self-healing** — Terminating every instance triggers ASG recovery; the system rebuilds itself from user-data scripts with zero manual intervention.

---

## Engineering Highlights

### Decoupled, event-driven processing
All major components communicate asynchronously through SNS topics and SQS queues. A slow annotator never backs up the web tier, and a web-tier deploy never drops in-flight jobs.

### Cost-optimized storage lifecycle
Free-tier results are archived to Glacier 5 minutes after completion via a delayed-message pattern. On upgrade, a two-phase thaw kicks in:
- `restore.py` initiates the retrieval (Expedited → Standard fallback)
- `thaw.py` polls, moves the restored object back to S3, and cleans up the vault
- Glacier archive IDs are persisted in DynamoDB to maintain a reliable bidirectional mapping

### Elastic compute with CloudWatch-driven policies
- **Web ASG** — scale out on 2XX response rate exceeding threshold; scale in on sub-10ms target response time
- **Annotator ASG** — scale out on `NumberOfMessagesSent > 50/10min`; scale in on `< 5/10min`
- Verified self-healing by terminating all `-web` and `-ann` instances and watching the environment fully rebuild

### Zero-hardcoding discipline
Bucket names, queue names, table names, and credentials are externalized into:
- `config.py` (Flask) via environment variables from `.env`
- `annotator_config.ini`, `archive_script_config.ini`, `thaw_script_config.ini`, and `util_config.ini` (utility scripts) via `ConfigParser`
- **AWS Secrets Manager** for DB credentials (never in source)

### Automated, reproducible deployment
ASG launch templates pull the latest source bundle from S3 and start the service from EC2 user-data — no SSH, no snowflake instances. A new region/account can be spun up from scratch.

---

## Tech Stack

| Category | Technologies |
|---|---|
| Languages | Python 3, HTML/Jinja2, Bash |
| Web | Flask, Gunicorn (multi-worker WSGI), Bootstrap |
| Data | DynamoDB, PostgreSQL, SQLAlchemy, Alembic |
| AWS | EC2, S3, Glacier, ELB, Auto Scaling, CloudWatch, Lambda, SES, SNS, SQS, RDS, Secrets Manager, ACM |
| IaC | Terraform (web/annotator/utility infrastructure, SNS/SQS, Lambda) |
| Testing | Locust (load testing), custom SQS load-generation script |

---

## Project Structure

```
gas/
├── web/                        # Flask web application
│   ├── app.py                  # Application factory
│   ├── views.py                # Routes and business logic
│   ├── config.py               # Runtime configuration
│   ├── auth.py                 # Globus Auth integration
│   ├── models.py               # SQLAlchemy models
│   ├── templates/              # Jinja2 templates
│   └── run_gas.sh              # Gunicorn launcher (port 4433)
├── ann/                        # Annotator service
│   ├── annotator.py            # SQS consumer → AnnTools
│   ├── run.py                  # Post-processing + SNS publish
│   ├── annotator_config.ini    # Shared annotator configuration
│   ├── run_ann.sh              # Runs the annotator script
│   ├── annotator_webhook.py    # (Optional) Flask webhook variant
│   └── run_ann_webhook.sh      # Runs webhook app (port 5000)
├── util/
│   ├── helpers.py              # Shared helper functions
│   ├── util_config.ini         # Common utility configuration
│   ├── ann_load.py             # Annotator load generator
│   ├── notify/                 # Job-completion email notifications
│   ├── archive/                # Free-user archival to Glacier
│   ├── thaw/                   # Glacier retrieval initiator
│   └── restore/                # Lambda: Glacier → S3 mover
├── aws/                        # EC2 user-data scripts
│   ├── user_data_web_server.txt
│   ├── user_data_annotator.txt
│   └── user_data_utils.txt
└── terraform/                  # Infrastructure as code
```

---

## Component Reference

### `/web` — Web Server

Flask-based web app for the GAS. Add routes and business logic in `views.py`; add or update Jinja2 templates in `/templates`. Constants (queue names, bucket names, etc.) must be declared in `config.py` and accessed via the `app.config` object.

The web server listens for requests on **port 4433**, as defined in `run_gas.sh`. The framework extends [Flask](https://flask.palletsprojects.com/) with [Globus Auth](https://docs.globus.org/api/auth) for federated login and [Bootstrap](https://getbootstrap.com/docs/3.3/) styling.

### `/ann` — Annotator

| File | Purpose |
|---|---|
| `annotator.py` | Annotator control script; spawns AnnTools runner |
| `run.py` | Runs AnnTools and updates environment on completion |
| `annotator_config.ini` | Shared configuration for `annotator.py` and `run.py` |
| `run_ann.sh` | Runs the annotator script |

For webhook-based deployments (alternative to the SQS polling script):

| File | Purpose |
|---|---|
| `annotator_webhook.py` | Annotator Flask app |
| `annotator_webhook_config.py` | Configuration for the webhook app |
| `run_ann_webhook.sh` | Runs the annotator Flask app on **port 5000** |

### `/util` — Utilities

Shared files at the top level:

| File | Purpose |
|---|---|
| `helpers.py` | Miscellaneous helper functions |
| `util_config.ini` | Common configuration for all utility scripts |
| `ann_load.py` | Annotator load-testing script |

Each utility lives in its own sub-directory with a configuration file and run script:

#### `/util/notify` — Job Completion Notifications

| File | Purpose |
|---|---|
| `notify.py` | Sends notification email on annotation job completion |
| `notify_config.ini` | Notification utility configuration |
| `run_notify.sh` | Runs the notification script |

#### `/util/archive` — Data Archival (Free Users)

| File | Purpose |
|---|---|
| `archive_script.py` | Archives free-user result files to Glacier |
| `archive_script_config.ini` | Archive utility configuration |
| `run_archive_script.sh` | Runs the archive script |

**Approach:** When a job completes, the annotator publishes to SNS topic `jfpan01_a14_archive_topic`; messages flow into SQS queue `jfpan01_a14_archive_queue`. The archive script polls the queue and, for each message, checks elapsed time since `complete_time`. If less than 180 seconds have passed, the message is requeued with SQS `DelaySeconds` (no blocking). After the grace period, it checks the user's current role — Premium users are skipped. For free users, it downloads from S3, uploads to Glacier, stores the archive ID in DynamoDB, and deletes from S3.

**Why a script, not a Flask app:** This is a long-running background service that only needs to poll a queue continuously. A script is lighter, easier to deploy, and matches the pattern used for other utilities. SQS `DelaySeconds` handles delays server-side so the script keeps processing other messages. User role is checked at archive time (not submission time), so users who upgrade during the grace period keep their files accessible.

#### `/util/thaw` — Glacier Retrieval Initiator

| File | Purpose |
|---|---|
| `thaw_script.py` | Initiates Glacier retrieval for archived objects |
| `thaw_script_config.ini` | Thaw utility configuration |
| `run_thaw_script.sh` | Runs the thaw script |

When a user upgrades via `/subscribe`, the web app queries DynamoDB for jobs with `results_file_archive_id` and publishes to SNS topic `jfpan01_a16_thaw_topic`. The thaw script polls SQS queue `jfpan01_a16_thaw_queue`, initiates Glacier retrieval jobs (Expedited first, Standard fallback on `InsufficientCapacityException`), and updates DynamoDB with `restore_job_id`, `restore_status`, `restore_tier`, and `restore_initiated_time`. SQS messages are deleted only after both Glacier initiation and DynamoDB update succeed.

#### `/util/restore` — Glacier → S3 Restoration (Lambda)

| File | Purpose |
|---|---|
| `restore.py` | AWS Lambda function that moves thawed objects back to S3 |

Triggered automatically by Glacier job completion via SNS topic `jfpan01_a16_glacier_restore`. The Lambda parses the Glacier notification, queries DynamoDB for the corresponding `job_id`, downloads the thawed archive, uploads it to the S3 results bucket with AES256 encryption, deletes the Glacier archive, and removes restoration-related fields from DynamoDB.

**Two-stage design rationale:**
- **Thaw script** — Lightweight, continuously polls the queue and initiates Glacier jobs. Configures Glacier to send SNS notifications on completion.
- **Restore Lambda** — Event-driven; only runs when Glacier notifies completion. No idle resources, no polling, auto-scales with concurrent restorations.

**Complete restoration flow:**
1. User upgrades to Premium → web app publishes to thaw SNS topic for each archived job
2. Thaw script receives message, initiates Glacier retrieval (Expedited → Standard fallback)
3. Glacier processes retrieval asynchronously
4. Glacier sends completion notification to `jfpan01_a16_glacier_restore` SNS topic
5. Lambda downloads archive, uploads to S3, deletes Glacier copy, cleans up DynamoDB
6. User downloads restored file from the job details page

### `/aws` — EC2 User Data

Bootstrap scripts for Auto Scaling Group launch templates:

| File | Purpose |
|---|---|
| `user_data_web_server.txt` | Configures instances launched by the web app ASG |
| `user_data_annotator.txt` | Configures instances launched by the annotator ASG |
| `user_data_utils.txt` | Configures utility instances to auto-run all utility scripts |

---

## Load Testing Results

Simulated 200–300 concurrent users at 20–30 req/sec using Locust against the ELB-fronted web tier:

- Web ASG scaled from 2 → near max (10) instances within ~2–3 minutes of sustained load
- Scale-in followed the configured cooldown window rather than reacting instantly — intentional, to avoid thrashing
- Annotator ASG scaled independently based on SQS queue depth, validating the decoupling between tiers

Key insight: CloudWatch-driven autoscaling lags observable user latency by 60–90 seconds in this setup. In production, I would pair reactive alarms with predictive scaling (or a smaller scaling step with shorter cooldown) to smooth over spike traffic.

---

## What I Took Away

- **Designing for failure** — every component can die, and the system must keep working
- **Thinking in queues and events** rather than synchronous calls
- **Cost vs. latency tradeoffs** — e.g., Expedited vs. Standard Glacier retrieval, ASG cooldown tuning
- **Idempotent bootstrapping** — encoding ops runbooks into user-data so infrastructure is disposable
- **Multi-tier access control** — OAuth2 federation + application-level role checks for Free/Premium separation

---

## License

Academic project. Not for commercial use.
