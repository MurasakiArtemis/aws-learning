import os

CONSTANTS = {
    'AWS': {
        'aws_access_key_id': os.environ['aws_access_key_id'],
        'aws_secret_access_key': os.environ['aws_secret_access_key'],
        'region_name': os.environ['aws_region_name'],
    },
    'KINESIS': {
        'STREAM': os.environ['kinesis_stream'],
    }
}
