import boto3
from boto3 import exceptions
from constants import CONSTANTS
import datetime
import base64
import json
import threading
import time
from decimal import Decimal


class KinesisConsumer(threading.Thread):
    """Generic Consumer for Amazon Kinesis Streams"""
    def __init__(self, 
        stream_name:str,
        shard_id:str,
        iterator_type:str,
        kinesis,
        table,
        name:str='KinesisConsumer',
        worker_time=30,
        sleep_interval=0.5,
    ):

        self.kinesis = kinesis
        self.table = table
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.iterator_type = iterator_type
        self.worker_time = worker_time
        self.sleep_interval = sleep_interval
        threading.Thread.__init__(self, name=name)


    def clean_dict(self, data):
        for key, value in list(data.items()):
            if isinstance(value, str) and not value:
                del data[key]
            if isinstance(value, float):
                data[key] = Decimal(value)
            elif isinstance(value, dict):
                self.clean_dict(value)
        return data

    def process_records(self, records):
        """the main logic of the Consumer that needs to be implemented"""
        for record in records:
            data = json.loads(record['Data'].decode('UTF-8'))
            clean_data = self.clean_dict(data)
            self.table.put_item(
                Item=clean_data
            )

    @staticmethod
    def iter_records(records):
        for record in records:
            part_key = record['PartitionKey']
            data = record['Data']
            yield part_key, data

    def run(self):
        """poll stream for new records and pass them to process_records method"""
        response = self.kinesis.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=self.shard_id, 
            ShardIteratorType=self.iterator_type
        )

        next_iterator = response['ShardIterator']

        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.worker_time)

        while finish > datetime.datetime.now():
            response = kinesis.get_records(
                ShardIterator=next_iterator,
                Limit=25
            )

            records = response['Records']

            if records:
                self.process_records(records)

            next_iterator = response['NextShardIterator']
            time.sleep(self.sleep_interval)

if __name__ == '__main__':
    kinesis = boto3.client(
        'kinesis',
        aws_access_key_id=CONSTANTS.get('AWS').get('aws_access_key_id'),
        aws_secret_access_key=CONSTANTS.get('AWS').get('aws_secret_access_key'),
        region_name=CONSTANTS.get('AWS').get('region_name'),
    )
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('tweets')
    shards = kinesis.list_shards(
            StreamName=CONSTANTS.get('KINESIS').get('STREAM')
        ).get('Shards')
    shard_id = shards[0].get('ShardId')
    kinesis_consumer = KinesisConsumer(
        CONSTANTS.get('KINESIS').get('STREAM'),
        shard_id,
        'LATEST',
        kinesis,
        table
    )
    kinesis_consumer.start()