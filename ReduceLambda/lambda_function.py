# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import json
import sys
import hashlib
import random
import time
from decimal import Decimal

# AWS Imports
import boto3
from botocore.exceptions import ClientError

# Project Imports
from functions import *
from constants import *
from performance_tracker import EventsCounter, PerformanceTrackerInitializer

# --------------------------------------------------------------------------------------------------
# Initialize Performance Tracker
# --------------------------------------------------------------------------------------------------

perf_tracker = PerformanceTrackerInitializer(True, INFLUX_CONNECTION_STRING, KIBANA_INSTANCE_IP)
event_counter = EventsCounter(['reduce_lambda_batch_size', 'reduce_lambda_message_count',
    'reduce_lambda_random_failures', 'end_to_end_latency_max', 'end_to_end_latency_mean'])

# --------------------------------------------------------------------------------------------------
# Lambda Function
# --------------------------------------------------------------------------------------------------

def lambda_handler(event, context):
    
    # Print Status at Start
    records = event['Records']
    print('Invoked ReduceLambda with ' + str(len(records)) + ' Delta message(s).')

    # Initialize Dict for Total Delta
    totals = dict()

    # Initialize DDB Ressource
    ddb_ressource = boto3.resource(DYNAMO_NAME, region_name=REGION_NAME)
    hash_table = ddb_ressource.Table(HASH_TABLE_NAME)
    
    # Calculate hash to ensure this batch hasn't been processed already:
    record_list_hash = hashlib.md5(str(records).encode()).hexdigest()

    # Keep track of number of batches for timestamp mean
    batch_count = 0
    
    # Iterate over Messages
    for record in event['Records']:

        # Aggregate over Batch of Messages the Lambda was invoked with
        if 'NewImage' in record[DYNAMO_NAME]:

            # Load Message to Dict
            message = record[DYNAMO_NAME]['NewImage']['Message']['S'].replace("'",'"')
            data = json.loads(message)

            # Get Batch Count (To Calculate Mean of Timestamp)
            batch_count += 1
    
            # Iterate over Entries in Message
            for entry in data:
                if entry == TIMESTAMP_GENERATOR_FIRST:
                    dict_entry_min(totals, entry, data[entry])
                else:
                    dict_entry_add(totals, entry, data[entry])

    # If this batch contains only deletes: Done
    if not totals:
        print('Skipped batch - no new entries.')
        return {'statusCode': 200}

    # Get Timestamps
    timestamp_generator_first = totals[TIMESTAMP_GENERATOR_FIRST]
    del totals[TIMESTAMP_GENERATOR_FIRST]
    timestamp_generator_mean = totals[TIMESTAMP_GENERATOR_MEAN] / batch_count
    del totals[TIMESTAMP_GENERATOR_MEAN]

    # Total Count of New Messages (for Printing)
    total_new_message_count = totals[MESSAGE_COUNT_NAME]
    
    # Read Current Values with one single BatchOperation
    key_list = [{AGGREGATE_TABLE_KEY: ident} for ident in totals.keys()]
    response = ddb_ressource.batch_get_item(
        RequestItems = {
            AGGREGATE_TABLE_NAME : {
                'ConsistentRead' : True,
                'AttributesToGet': [AGGREGATE_TABLE_KEY, VALUE_COLUMN_NAME],
                'Keys': key_list
            }
        })

    # Increment Counts
    for entry in response['Responses'][AGGREGATE_TABLE_NAME]:
        ident = entry[AGGREGATE_TABLE_KEY]
        totals[ident] = totals[ident] + float(entry[VALUE_COLUMN_NAME])
    
    # Write New Values as Transaction
    ddb_client = boto3.client(DYNAMO_NAME, region_name=REGION_NAME)
    
    # Normal Items
    batch = [ { 'Put': { 'Item': {AGGREGATE_TABLE_KEY: {'S' : entry}, 
        VALUE_COLUMN_NAME: {'N' : str(totals[entry])}}, 
        'TableName' : AGGREGATE_TABLE_NAME } } for entry in totals.keys()]

    try:
        response = ddb_client.transact_write_items(
            TransactItems = batch,
            ClientRequestToken = record_list_hash
        )
    except ClientError as e:
        if e.response['Error']['Code']=='IdempotentParameterMismatchException':  
            print('Batch was already processed. Skipping this one.')
            return {'statusCode': 200}
        else:
            raise Exception(e)
        

    # Performance Tracker
    event_counter.increment('reduce_lambda_batch_size', len(records))
    event_counter.increment('reduce_lambda_message_count', total_new_message_count)
    event_counter.increment('end_to_end_latency_max', 
        float(time.time() - timestamp_generator_first))
    event_counter.increment('end_to_end_latency_mean', 
        float(time.time() - timestamp_generator_mean))

    # Manually Introduced Random Failure    
    if random.uniform(0,100) < FAILURE_REDUCE_LAMBDA_PCT:

        # Submit Performance Measurements
        event_counter.increment('reduce_lambda_random_failures', 1)
        perf_tracker.add_metric_sample(None, event_counter, None, None)
        perf_tracker.submit_measurements()
        
        # Raise Exception
        raise Exception('Manually Introduced Random Failure!')

    # Submit Performance Measurements
    perf_tracker.add_metric_sample(None, event_counter, None, None)
    perf_tracker.submit_measurements()

    # Print Status at End
    print('ReduceLambda finished. Updates aggregates with ' + str(total_new_message_count) + ' new message(s) in total.')

    return {'statusCode': 200}