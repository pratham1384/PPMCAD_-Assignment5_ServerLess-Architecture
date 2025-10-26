import boto3
import os
from datetime import datetime

ec2 = boto3.client('ec2')

# environment-configurable
CUSTOM_TAG_KEY = os.environ.get('CUSTOM_TAG_KEY', 'Owner')
CUSTOM_TAG_VALUE = os.environ.get('CUSTOM_TAG_VALUE', 'DevTeam')
DATE_TAG_KEY = os.environ.get('DATE_TAG_KEY', 'LaunchDate')

def extract_instance_ids_from_runinstances(event):
    # CloudTrail RunInstances -> event['detail'] may contain 'responseElements' with instancesSet
    ids = []
    detail = event.get('detail', {})
    # CloudTrail RunInstances (if CloudTrail is sending to EventBridge)
    if detail.get('eventName') == 'RunInstances' or event.get('detail-type') == 'AWS API Call via CloudTrail':
        resp = detail.get('responseElements') or {}
        instances = resp.get('instancesSet') or resp.get('instancesSet', {})
        # try several possible shapes
        if isinstance(instances, dict):
            # sometimes instancesSet: { 'items': [ { 'instanceId': 'i-...' }, ...] }
            items = instances.get('items') or instances.get('Instances') or []
            if isinstance(items, list):
                for it in items:
                    iid = it.get('instanceId') or it.get('instanceId')
                    if iid:
                        ids.append(iid)
        elif isinstance(instances, list):
            for it in instances:
                iid = it.get('instanceId')
                if iid:
                    ids.append(iid)
    # also try detail.responseElements.instancesSet.items
    try:
        items = detail.get('responseElements', {}).get('instancesSet', {}).get('items', [])
        for it in items:
            iid = it.get('instanceId')
            if iid:
                ids.append(iid)
    except Exception:
        pass
    return list(set(ids))

def extract_instance_ids_from_state_change(event):
    # EventBridge EC2 Instance State-change Notification
    ids = []
    detail = event.get('detail', {})
    if detail.get('state') == 'running':
        instance_id = detail.get('instance-id') or detail.get('instanceId') or detail.get('instanceId')
        if instance_id:
            ids.append(instance_id)
    return ids

def lambda_handler(event, context):
    print("Received event:", event)
    instance_ids = []

    # Try either event formats
    instance_ids += extract_instance_ids_from_runinstances(event)
    instance_ids += extract_instance_ids_from_state_change(event)

    # Deduplicate
    instance_ids = list(set(instance_ids))

    if not instance_ids:
        # fallback: sometimes event contains 'resources' list with arn with instance id
        for r in event.get('resources', []) :
            if isinstance(r, str) and r.startswith('arn:aws:ec2'):
                # arn:aws:ec2:region:acct:instance/i-abcdef
                parts = r.split('/')
                if len(parts) >= 2:
                    instance_ids.append(parts[-1])
        instance_ids = list(set(instance_ids))

    if not instance_ids:
        print("No instance IDs found in the event. Exiting.")
        return {"status": "no-instance-found"}

    # Tagging payload
    today = datetime.utcnow().date().isoformat()  # UTC date like '2025-10-23'
    tags = [
        {'Key': DATE_TAG_KEY, 'Value': today},
        {'Key': CUSTOM_TAG_KEY, 'Value': CUSTOM_TAG_VALUE}
    ]

    try:
        print(f"Tagging instances: {instance_ids} with tags: {tags}")
        ec2.create_tags(Resources=instance_ids, Tags=tags)
        print(f"Successfully created tags on {instance_ids}")
        return {"status": "success", "instances": instance_ids, "tags": tags}
    except Exception as e:
        print("Error tagging instances:", str(e))
        raise
