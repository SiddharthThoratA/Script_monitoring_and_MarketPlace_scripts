import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError


def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        #s3.delete_object(local_file, bucket, s3_file)
        print("********************************************************")
        print(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
        return True
    except FileNotFoundError:
        print(f"The file was not found: {local_file}")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Failed to upload {local_file} to {bucket}/{s3_file}: {e}")
        return False
        

def multi_files_upload_s3(local_file_path,s3_bucket,s3_path):
    for root,dirs,files in os.walk(local_file_path):
        if root == local_file_path:
            for file in files:
                if file.endswith(".csv") or file.endswith(".json"):
                    local_file_paths = os.path.join(root,file)
                    print("Local file path is:",local_file_paths)
                    s3_file_path = os.path.join(s3_path,file)
                    print('s3 file path is:',s3_file_path)
                    upload_to_s3(local_file_paths,s3_bucket,s3_file_path)
                
def delete_from_s3(bucket_name, s3_file_key):
    try:
        # Initialize a session using Amazon S3
        s3 = boto3.client('s3')
        
        # Delete the file
        s3.delete_object(Bucket=bucket_name, Key=s3_file_key)
        
        print(f"Delete Successful: {bucket_name}/{s3_file_key}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Failed to delete {bucket_name}/{s3_file_key}: {e}")
        return False


def create_empty_file_on_s3(bucket_name, s3_file_full_path):
    try:
        # Initialize a session using Amazon S3
        s3 = boto3.client('s3')
        
        # Create an empty object
        s3.put_object(Bucket=bucket_name, Key=s3_file_full_path, Body='')
        
        print(f"Empty file created: {bucket_name}/{s3_file_full_path}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Failed to create empty file {bucket_name}/{s3_file_full_path}: {e}")
        return False
        
        

def maths(a,b):
    return a + b
    