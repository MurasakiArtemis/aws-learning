import argparse
import boto3
import logging
import os
from botocore.exceptions import ClientError
from constants import CONSTANTS


ACCESS_KEY = CONSTANTS.get('aws').get('aws_access_key_id')
SECRET_KEY = CONSTANTS.get('aws').get('aws_secret_access_key')


def put_object(s3, dest_bucket_name, dest_object_name, src_data):
    if isinstance(src_data, str):
        try:
            object_data = open(src_data, 'rb')
            # possible FileNotFoundError/IOError exception
        except Exception as e:
            logging.error(e)
            return False
    else:
        logging.error(
            f'Type of {str(type(src_data))} for the argument '
            '\'src_data\' is not supported.'
        )
        return False
    try:
        s3.put_object(
            Bucket=dest_bucket_name,
            Key=dest_object_name,
            Body=object_data
        )
    except ClientError as e:
        logging.error(e)
        return False
    finally:
        if isinstance(src_data, str):
            object_data.close()
    return True


def process_directory(s3, bucket, base_key, directory):
    current_directory = os.path.basename(directory)
    for name in os.listdir(directory):
        full_path = os.path.join(directory, name)
        if os.path.isdir(full_path):
            process_directory(
                s3,
                bucket,
                os.path.join(base_key, current_directory),
                full_path,
            )
        else:
            key = os.path.join(base_key, name)
            success = put_object(
                s3,
                bucket,
                key,
                full_path,
            )
            if success:
                logging.info(f'Added {key} to {bucket}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'bucket',
        help='Bucket on which to store',
    )
    parser.add_argument(
        'filename',
        help='File to upload',
    )
    parser.add_argument(
        'key',
        help='Object key',
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='Print information for every uploaded object',
        action='store_true',
    )
    args = parser.parse_args()
    bucket_name = args.bucket
    filename = args.filename
    object_name = args.key
    if args.verbose:
        logging.basicConfig(
            level=logging.INFO,
            format='%(levelname)s: %(asctime)s: %(message)s'
        )
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    if os.path.isdir(filename):
        process_directory(s3, bucket_name, object_name, filename)
    else:
        success = put_object(s3, bucket_name, object_name, filename)
        if success:
            logging.info(f'Added {object_name} to {bucket_name}')


if __name__ == '__main__':
    main()
