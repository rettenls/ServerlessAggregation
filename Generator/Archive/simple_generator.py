# General Imports
import random
import json
import hashlib
import time
import collections
import uuid
import sys
from pprint import pprint

# Multithreading
import logging
import threading

# AWS Imports
import boto3

# Project Imports
sys.path.append("../Common")
from functions import *
from constants import *

# Random Type Constructor
def random_type():
    type_name = None
    for type_level in type_levels:
        if type_name is None:
            type_name = random.choice(type_level) 
        elif type_name in type_level:
            type_name += ":" + random.choice(type_level[type_name])
    return type_name

def generate_messages(print_to_console):
    
    for i in range(NUMBER_OF_BATCHES_PER_THREAD):
        
        # The designated thread should print current progress
        if print_to_console:
            progress = (i / NUMBER_OF_BATCHES_PER_THREAD) * 100
            print_progress_bar(progress)
    
        # Initialize record list for this batch
        records = []
        
        for j in range(BATCH_SIZE):
            
            # Initialize Empty Message
            message = {}
            
            # Generate ID
            message["id"] = str(uuid.uuid4())
            
            # Add Version
            message["version"] = 0
            
            # Add Value
            v = random.randint(1, MAX_NUMBER_OF_ITEMS_PER_MESSAGE)
            message["value"] = v
            
            # Add Random Type
            #k = "fruit:banana:yellow"
            k = random_type()
            message["type"] = k
            
            # Dump to String
            message_string = json.dumps(message)
            
            # Append to Record List
            record = {"Data" : message_string, "PartitionKey" : hashlib.sha256(message_string.encode()).hexdigest()}
            records.append(record)

        # Send Batch to Kinesis Stream
        batch_put_with_exp_backoff(kinesis_client, KINESIS_STREAM_NAME, records)
    

# Initialize Kinesis Consumer
kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)

# Take start time
start_time = time.time()

# Print general info
print("\nGenerating items and writing to Kinesis...\n")
print("Example message: {'id': '0d957288-2913-4dbb-b359-5ec5ff732cac', 'version': 0, 'value': 1, 'type': 'vegetable:cucumber'}\n")

# Invoke Threads
threads = list()

print("Invoking " + str(THREAD_NUM) + " threads...\n")
for index in range(THREAD_NUM):
    x = threading.Thread(target=generate_messages, args=(index == (THREAD_NUM - 1),))
    threads.append(x)
    x.start()

for index, thread in enumerate(threads):
    thread.join()
    
print("\n\nAll threads finished.\n")
    
# Print to Console
end_time = time.time()
print("\nSimple Data producer finished!\nTotal number of messages: {}.\nTotal ingestion time: {:.1f} seconds.\nAverage ingeston rate: {:.1f} messages / second.".format(BATCH_SIZE * NUMBER_OF_BATCHES_PER_THREAD * THREAD_NUM, end_time - start_time, BATCH_SIZE * NUMBER_OF_BATCHES_PER_THREAD * THREAD_NUM / (end_time - start_time)))