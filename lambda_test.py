import json
import boto3
import base64


def clean_dict(data):
    for key, value in list(data.items()):
        if not value:
            del data[key]
        elif isinstance(value, dict):
            clean_dict(value)
    return data

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('tweets')

    for record in event['Records']:
        try:
            payload = base64.b64decode(record["kinesis"]["data"])
            data = json.loads(str(payload, 'UTF-8'))
            clean_data = clean_dict(data)
            table.put_item(
                Item=clean_data
            )
        except:
            pass
    return 'Lambda Finished'
