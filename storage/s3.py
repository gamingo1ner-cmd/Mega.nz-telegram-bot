import boto3

def get_client():
    return boto3.client("s3")
