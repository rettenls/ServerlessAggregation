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

# Aggregate along tree
def aggregate_along_tree(data):
    
    # Determine aggregation depth
    aggregation_depth = max([key.count(":") for key in data.keys()])
    
    # Start at max depth and go higher 
    for depth in range(aggregation_depth, 0, -1):
        children = [key for key in data.keys() if key.count(":") == depth]
        for child in children:
            parent = child[:child.rfind(":")]
            add_to_dict_entry(data, parent, data[child])
                
    return data

def generate_messages(totals, print_to_console):
    
    thread_state = dict()
    thread_totals = dict()
    
    for i in range(NUMBER_OF_BATCHES_PER_THREAD):
        
        # The designated thread should print current progress
        if print_to_console:
            progress = (i / NUMBER_OF_BATCHES_PER_THREAD) * 100
            print_progress_bar(progress)
    
        # Initialize record list for this batch
        records = []
        
        # Calcu late number of duplicates that we add
        if DUPLICATES_PER_BATCH < BATCH_SIZE:
            number_of_duplicate_messages = DUPLICATES_PER_BATCH
        else:
            number_of_duplicate_messages = max(0, BATCH_SIZE - 1)
        
        for j in range(BATCH_SIZE - number_of_duplicate_messages):
            
            # Initialize Empty Message
            message = {}
            
            # Random decision: 10% chance for Modify, 90% chance for New Entry
            if len(thread_state) == 0 or random.uniform(0,100) < (100 - PERCENTAGE_MODIFY):
                # Generate ID
                message["id"] = str(uuid.uuid4())
                
                # Add Version
                message["version"] = 0
                
                # Count
                add_to_dict_entry(thread_totals, "count:add", 1)
                
            else:
                # Pick existing ID
                message["id"] = random.choice(list(thread_state.keys()))
                
                # Get New Version
                if thread_state[message["id"]]["version"] == 0 or random.uniform(1,100) < (100 - PERCENTAGE_OUT_OR_ORDER):
                    # Iterate Version
                    message["version"] = thread_state[message["id"]]["version"] + 1
                    add_to_dict_entry(thread_totals, "count:modify:in_order", 1)
                else:
                    print("\nOut or Order Message!\n")
                    # Insert Older Version
                    message["version"] = thread_state[message["id"]]["version"] - 1
                    add_to_dict_entry(thread_totals, "count:modify:out_of_order", 1)
                
            # Add Random Value
            v = random.randint(1, MAX_NUMBER_OF_ITEMS_PER_MESSAGE)
            message["value"] = v
            
            # Add Random Type
            k = random_type()
            message["type"] = k
            
            # Dump to String
            message_string = json.dumps(message)
            
            # Append to Record List
            record = {"Data" : message_string, "PartitionKey" : hashlib.sha256(message_string.encode()).hexdigest()}
            records.append(record)
            
            # Append to Internal Storage - if Message was sent in Order
            if (message["id"] not in thread_state) or (thread_state[message["id"]]["version"] < message["version"]):
                thread_state[message["id"]] = message
    
        # Add Duplicates
        for k in range(number_of_duplicate_messages):
            duplicate_index = random.randint(0, BATCH_SIZE - number_of_duplicate_messages - 1)
            records.append(records[duplicate_index])
        
        add_to_dict_entry(thread_totals, "count:duplicates", number_of_duplicate_messages)

        #Print records
        if DEBUG > 1:
            print('\n')
            pprint(records)
            print('\n')

        # Send Batch to Kinesis Stream
        batch_put_with_exp_backoff(kinesis_client, KINESIS_STREAM_NAME, records)
        
    # Aggregate over Final State
    for entry in thread_state.values():
        k = entry['type']
        v = entry['value']
        add_to_dict_entry(thread_totals, k, v)

    # Add to Totals
    for k,v in thread_totals.items():
        add_to_dict_entry(totals, k, v)
        

# Initialize Kinesis Consumer
kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)

# Take start time
start_time = time.time()

# Print general info
print("\nGenerating items and writing to Kinesis...\n")
print("Example message: {'id': '0d957288-2913-4dbb-b359-5ec5ff732cac', 'version': 0, 'value': 1, 'type': 'vegetable:cucumber'}\n")

# Invoke Threads
totals = dict()
threads = list()

print("Invoking " + str(THREAD_NUM) + " threads...\n")
for index in range(THREAD_NUM):
    x = threading.Thread(target=generate_messages, args=(totals, index == (THREAD_NUM - 1),))
    threads.append(x)
    x.start()

for index, thread in enumerate(threads):
    thread.join()
    
print("\n\nAll threads finished.\n")
    
# Print to Console
end_time = time.time()
print("\nSimple Data producer finished!\nTotal number of messages: {}.\nTotal ingestion time: {:.1f} seconds.\nAverage ingeston rate: {:.1f} messages / second.".format(BATCH_SIZE * NUMBER_OF_BATCHES_PER_THREAD * THREAD_NUM, end_time - start_time, BATCH_SIZE * NUMBER_OF_BATCHES_PER_THREAD * THREAD_NUM / (end_time - start_time)))
# Print Totals to Compare
totals = aggregate_along_tree(totals)

ordered_totals = collections.OrderedDict(sorted(totals.items()))
print("\nMessage Counts:\n")
for k,v in ordered_totals.items():
    if k[:5] == "count":
        level = k.count(":") 
        print("{:<25}".format(k) + (" " * level) + "{:>10}".format(v))
print("\n")

print("\nTotals:\n")
for k,v in ordered_totals.items():
    if k[:5] != "count":
        level = k.count(":") 
        print("{:<25}".format(k) + (" " * level) + "{:>10}".format(v))
print("\n")