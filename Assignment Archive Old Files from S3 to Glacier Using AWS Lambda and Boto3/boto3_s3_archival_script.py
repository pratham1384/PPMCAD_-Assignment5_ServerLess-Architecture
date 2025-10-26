import logging
from datetime import datetime, timezone, timedelta

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ====== CONFIGURATION ======
BUCKET = "pratham-static-site01"   # Fixed bucket name
DAYS_OLD = 180                     # Files older than 6 months
TARGET_STORAGE_CLASS = "GLACIER"   # Archive storage class
MULTIPART_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5 GiB threshold
# ============================

# AWS clients
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

def lambda_handler(event, context):
    """
    Entry point expected by AWS Lambda (lambda_function.lambda_handler)
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_OLD)
    logger.info(f"Starting archive run for bucket={BUCKET}, cutoff={cutoff.isoformat()}, storage_class={TARGET_STORAGE_CLASS}")

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET)

    archived = 0
    errors = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            last_modified = obj["LastModified"]
            size = obj["Size"]
            current_storage_class = obj.get("StorageClass", "STANDARD")

            logger.debug(f"Found object: {key}, last_modified={last_modified}, size={size}, storage_class={current_storage_class}")

            # Skip if already archived
            if current_storage_class in ("GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"):
                logger.info(f"Skipping {key} — already in {current_storage_class}")
                continue

            # Skip if not older than threshold
            if last_modified >= cutoff:
                continue

            try:
                copy_source = {"Bucket": BUCKET, "Key": key}

                # Small file (<= 5GB)
                if size <= MULTIPART_THRESHOLD:
                    s3_client.copy_object(
                        Bucket=BUCKET,
                        Key=key,
                        CopySource=copy_source,
                        StorageClass=TARGET_STORAGE_CLASS,
                        MetadataDirective="COPY"
                    )
                else:
                    # Large file (> 5GB) - use multipart copy
                    config = TransferConfig(
                        multipart_threshold=MULTIPART_THRESHOLD,
                        multipart_chunksize=1024 * 1024 * 1024
                    )
                    transfer = S3Transfer(s3_client, config)
                    extra_args = {
                        "StorageClass": TARGET_STORAGE_CLASS,
                        "MetadataDirective": "COPY"
                    }
                    transfer.copy(copy_source, BUCKET, key, extra_args=extra_args)

                # Copy object tags if present
                try:
                    tagging = s3_client.get_object_tagging(Bucket=BUCKET, Key=key)
                    tag_set = tagging.get("TagSet", [])
                    if tag_set:
                        s3_client.put_object_tagging(Bucket=BUCKET, Key=key, Tagging={"TagSet": tag_set})
                except Exception:
                    logger.warning(f"Could not copy tags for {key} (may not exist or no permission)")

                logger.info(f"Archived {key} ({size} bytes) to {TARGET_STORAGE_CLASS}")
                archived += 1

            except Exception as e:
                logger.exception(f"Failed to archive {key}: {e}")
                errors += 1

    logger.info(f"Archive run complete. Archived: {archived}, Errors: {errors}")
    return {"archived": archived, "errors": errors}
