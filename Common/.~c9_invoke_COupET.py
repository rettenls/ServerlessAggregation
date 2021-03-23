# General Imports
from time import sleep
from pprint import pprint

# AWS Imports
import boto3

# Own Imports
from constants import *

# Add to Dict Entry
def add_to_dict_entry(d, k, v):
    if k in d:
        d[k] += v
    else:
        d[k] = v

# Batch write to DDB table with Exponential Backoff
def batch_write_with_exp_backoff(dynamodb, request_items):

    # First Try
    response = dynamodb.batch_write_item(RequestItems = request_items)

    # Exponential Backoff
    sleep_cycles = 1
    while response['UnprocessedItems']:
        sleep(0.05 * (sleep_cycles ** 2))
        request_items = response['UnprocessedItems']
        response = dynamodb.batch_write_item(RequestItems = request_items)
        sleep_cycles += 1
        
# Batch put to Kinesis Stream with Exponential Backoff
def batch_put_with_exp_backoff(kinesis_client, stream_name, records):

    # First Try
    response = kinesis_client.put_records(StreamName=stream_name,Records=records)
    
    if DEBUG > 1:
        pprint(records)
        pprint(response)

    # Exponential Backoff
    sleep_cycles = 1
    while response["FailedRecordCount"]:
        sleep(0.05 * (sleep_cycles ** 2))
        remaining_records =[records[i] for i in range(len(records)) if "ErrorCode" in response["Records"][i]]
        records = remaining_records
        print("\n\nRetrying " + str(response["FailedRecordCount"]) + " items:\n")
        pprint(records)
        response = kinesis_client.put_records(StreamName=stream_name,Records=records)
        sleep_cycles += 1
        
# Lookup item by key in DynamoDB Table
def exists(table, key_name):
    
    # Call to DynamoDB
    response = table.get_item(Key=key_name)

    if 'Item' in response:
        item = response['Item']
    else:
        item = None
   
    return item

# Print Progress Bar
def print_progress_bar(progress):
    progress_bar = int(progress / 2) + 1
    print("\r[" + "#" * progress_bar + " " * (50 - progress_bar) + "]   {:.1f}% completed.".format(progress), end = "")