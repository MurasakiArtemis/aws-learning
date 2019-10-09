import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging


glueContext = GlueContext(SparkContext.getOrCreate())

db_name = 'twitter_dynamodb'
tbl_tweets = 'test_streamtweets'

s3_tweets = 's3://aws-glue-murasaki-artemis-twitter-stream/output-dir/tweets'
s3_tweets_tmp = 's3://aws-glue-murasaki-artemis-twitter-stream/tmp/tweets'
s3_users = 's3://aws-glue-murasaki-artemis-twitter-stream/output-dir/users'
s3_users_tmp = 's3://aws-glue-murasaki-artemis-twitter-stream/tmp/users'

## @type: DataSource
## @args: [database = "twitter_dynamodb", table_name = "test_streamtweets"]
## @return: tweets
## @inputs: []
tweets = glueContext.create_dynamic_frame.from_catalog(
    database = db_name,
    table_name = tbl_tweets,
)

logging.info('Got Context')

def process_tweet(record):
    if 'truncated' in record:
        start = record['extended_tweet']['display_text_range'][0]
        end = record['extended_tweet']['display_text_range'][1]
        record['text'] = record['extended_tweet']['full_text'][start:end]
    if 'extended_tweet' in record:
        record['user_mentions'] = [
            user_mention['id_str'] 
            for user_mention in record['extended_tweet']['entities']['user_mentions']
        ]
        record['hashtags'] = [
            hashtag['text']
            for hashtag in record['extended_tweet']['entities']['hashtags']
        ]
        del record['extended_tweet']
    elif 'entities' in record:
        record['user_mentions'] = [
            user_mention['id_str'] 
            for user_mention in record['entities']['user_mentions']
        ]
        record['hashtags'] = [
            hashtag['text']
            for hashtag in record['entities']['hashtags']
        ]
        del record['entities']
    return record

## @type: SelectFields
## @args: [paths = ["user"]]
## @return: users
## @inputs: [frame = tweets]
users = SelectFields.apply(frame = tweets, paths = ['user'])

## @type: SelectFields
## @args: [paths = ["paths"]]
## @return: tweets
## @inputs: [frame = tweets]
tweets = SelectFields.apply(
    frame = tweets, 
    paths = [
        'id',
        'id_str',
        'text',
        'extended_tweet'
        'lang',
        'timestamp_ms',
        'created_at',
        'in_reply_to_user_id_str',
        'in_reply_to_user_id',
        'in_reply_to_status_id_str',
        'in_reply_to_status_id',
        'possibly_sensitive',
    ], 
)
logging.info('Selected Fields')
## @type: Map
## @args: [f = process_tweet]
## @return: tweets
## @inputs: [frame = tweets]
tweets = Map.apply(frame = tweets, f = process_tweet)
logging.info('Mapped')
## @type: Relationalize
## @args: [staging_path = "s3://glue_twitter_stream/tmp/tweets", name = "tweets"]
## @return: tweets
## @inputs: [frame = tweets]
tweets = Relationalize.apply(frame = tweets,
    staging_path = s3_tweets_tmp, name = 'tweets')

users = users.unnest()
logging.info('Unnested')
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path":"s3://glue_twitter_stream/output-dir/tweets"}, format = "parquet"]
## @return: tweets
## @inputs: [frame = tweets]
glueContext.write_dynamic_frame.from_options(
    frame=tweets,
    connection_type='s3',
    connection_options={'path': s3_tweets},
    format='parquet',
)

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path":"s3://glue_twitter_stream/output-dir/users"}, format = "parquet"]
## @return: users
## @inputs: [frame = tweets]
glueContext.write_dynamic_frame.from_options(
    frame=users,
    connection_type='s3',
    connection_options={'path': s3_users},
    format='parquet',
)
logging.info('Wrote')