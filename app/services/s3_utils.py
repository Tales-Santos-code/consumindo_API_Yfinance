import boto3
import os

def get_s3_client():
    return boto3.client('s3')  #~/.aws/credentials

def upload_file_to_s3(local_path, bucket_name, s3_key):
    s3 = get_s3_client()
    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"✅ Enviado para S3: {local_path} → s3://{bucket_name}/{s3_key}")

def download_file_from_s3(bucket_name, s3_key, local_path):
    s3 = get_s3_client()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket_name, s3_key, local_path)
    print(f"✅ Baixado do S3: s3://{bucket_name}/{s3_key} → {local_path}")
