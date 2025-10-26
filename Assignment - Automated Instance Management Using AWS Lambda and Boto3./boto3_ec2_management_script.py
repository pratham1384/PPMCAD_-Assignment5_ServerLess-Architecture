import boto3

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')

    # --- Handle Auto-Stop Instances ---
    stop_filter = [{'Name': 'tag:Action', 'Values': ['Auto-Stop']}]
    stop_instances = ec2.describe_instances(Filters=stop_filter)

    stop_ids = []
    for reservation in stop_instances['Reservations']:
        for instance in reservation['Instances']:
            if instance['State']['Name'] != 'stopped':
                stop_ids.append(instance['InstanceId'])

    if stop_ids:
        print(f"Stopping instances: {stop_ids}")
        ec2.stop_instances(InstanceIds=stop_ids)
    else:
        print("No instances to stop.")

    # --- Handle Auto-Start Instances ---
    start_filter = [{'Name': 'tag:Action', 'Values': ['Auto-Start']}]
    start_instances = ec2.describe_instances(Filters=start_filter)

    start_ids = []
    for reservation in start_instances['Reservations']:
        for instance in reservation['Instances']:
            if instance['State']['Name'] != 'running':
                start_ids.append(instance['InstanceId'])

    if start_ids:
        print(f"Starting instances: {start_ids}")
        ec2.start_instances(InstanceIds=start_ids)
    else:
        print("No instances to start.")

    return {
        'statusCode': 200,
        'body': f"Stopped: {stop_ids}, Started: {start_ids}"
    }
