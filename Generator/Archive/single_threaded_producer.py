# General Imports
import random
import json
import hashlib
import time
import collections

# AWS Imports
import boto3

# GENERAL
AWS_REGION = "eu-central-1"
STREAM_NAME = "FruitBasketStream"

# SIZES
NUMBER_OF_BATCHES_PER_PROCESS = 1000
BATCH_SIZE = 500
MAX_NUMBER_OF_ITEMS_PER_MESSAGE = 10

# Definition of Fruit Types
type_level1 = ["fruit", "vegetable"]
type_level2 = {"fruit": ["apple", "pear", "banana"], "vegetable": ["tomato", "cucumber", "potato"]}
type_level3 = {"fruit:apple": ["red", "green"], "fruit:banana": ["green", "yellow"], "vegetable:tomato":["small", "large"]}
type_levels = [type_level1, type_level2, type_level3]

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
            if parent not in data:
                data[parent] = data[child]
            else:
                data[parent] += data[child]
                
    return data

# Initialize Kinesis Producer
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

# Keep track of generated items
totals = dict()

# Take start time
start_time = time.time()

# Print general info
print("\nGenerating items and writing to Kinesis...\n")
print("Example message: {'type': 'vegetable:tomato:large', 'count': 3, 'timestamp': 1607973049.6147885}\n\n")
for i in range(NUMBER_OF_BATCHES_PER_PROCESS):
    
    # Print current progress
    progress = (i / NUMBER_OF_BATCHES_PER_PROCESS) * 100
    progress_bar = int(progress / 2) + 1
    print("\r[" + "#" * progress_bar + " " * (50 - progress_bar) + "]   {:.1f}% completed.".format(progress), end = "")

    # Initialize record list for this batch
    records = []
    
    for j in range(BATCH_SIZE):
        
        # Initialize Empty Message
        message = {}
        
        # Add Random Type
        k = random_type()
        message["type"] = k
        
        # Add Random Count
        v = random.randint(1, MAX_NUMBER_OF_ITEMS_PER_MESSAGE)
        message["count"] = v
        
        # Add Timestamp
        message["timestamp"] = time.time()
        
        # Dump to String
        message = json.dumps(message)
        
        # Append to Record List
        record = {"Data" : message, "PartitionKey" : hashlib.sha256(message.encode()).hexdigest()}
        records.append(record)
        
        # Add to totals
        if k not in totals.keys():
            totals[k] = v
        else:
            totals[k] += v
        
    # Send Batch to Kinesis Stream
    put_response = kinesis_client.put_records(StreamName=STREAM_NAME,Records=records)
    
# Print to Console
end_time = time.time()
print("\n\nData producer finished!\nTotal number of messages: {},\nTotal ingestion time: {:.1f} seconds.".format(NUMBER_OF_BATCHES_PER_PROCESS*BATCH_SIZE, end_time - start_time))

# Print Totals to Compare
totals = aggregate_along_tree(totals)
ordered_totals = collections.OrderedDict(sorted(totals.items()))
print("\nTotals:\n")
for k,v in ordered_totals.items():
    level = k.count(":") 
    print("{:<25}".format(k) + (" " * level) + "{:>10}".format(v))
print("\n")