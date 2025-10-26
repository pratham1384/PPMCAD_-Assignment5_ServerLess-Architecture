import os
import logging
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# Read configuration from environment
BUCKET = os.environ.get('BUCKET_NAME')
DAYS = int(os.environ.get('DAYS_THRESHOLD', '30'))
PREFIX = os.environ.get('PREFIX', None)  # optional

DELETE_BATCH_SIZE = 1000  # S3 delete_objects supports up to 1000 keys per request

def _delete_batch(bucket, objects_to_delete):
    """Call S3 delete_objects on a list of {'Key': key} items."""
    if not objects_to_delete:
        return []
    try:
        resp = s3.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
        deleted = resp.get('Deleted', [])
        errors = resp.get('Errors', [])
        if errors:
            logger.warning("Errors returned by delete_objects: %s", errors)
        return deleted
    except ClientError as e:
        logger.exception("Failed to delete objects: %s", e)
        raise

def lambda_handler(event, context):
    if not BUCKET:
        msg = "Environment variable BUCKET_NAME is not set"
        logger.error(msg)
        return {"status": "error", "message": msg}

    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS)
    logger.info("Running cleanup for bucket=%s; removing objects older than %s (UTC)", BUCKET, cutoff.isoformat())

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX) if PREFIX else paginator.paginate(Bucket=BUCKET)

    to_delete = []
    total_deleted = 0
    deleted_keys = []

    for page in page_iterator:
        contents = page.get('Contents', [])
        for obj in contents:
            key = obj['Key']
            last_modified = obj['LastModified']  # this is a timezone-aware datetime
            if last_modified < cutoff:
                to_delete.append({'Key': key})
                # flush when hitting batch size
                if len(to_delete) >= DELETE_BATCH_SIZE:
                    deleted = _delete_batch(BUCKET, to_delete)
                    total_deleted += len(deleted)
                    deleted_keys.extend([d.get('Key') for d in deleted])
                    to_delete = []

    # final flush
    if to_delete:
        deleted = _delete_batch(BUCKET, to_delete)
        total_deleted += len(deleted)
        deleted_keys.extend([d.get('Key') for d in deleted])

    logger.info("Cleanup finished. Total objects deleted: %d", total_deleted)
    logger.info("Deleted keys: %s", deleted_keys[:100])  # truncate log if many

    return {
        "status": "success",
        "deleted_count": total_deleted,
        "deleted_sample": deleted_keys[:100]
    }
