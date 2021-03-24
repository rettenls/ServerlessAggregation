# Serverless Aggregation Architecture

## The Modules / Directories
This project currently consists of a DataGenerator, a Frontend, and three Lambda functions: StateLambda, MapLambda and ReduceLambda.

## Getting Started

1. Clone this repo - ideally into a Cloud9 environment: "git clone https://github.com/lucasrettenmeier/ServerlessAggregation.git".
2. Make sure Python3 and pip3 are installed.
3. Run "chmod +x requirements.sh" and "./requirements.sh" to install required Python3 libraries.

## Running the Frontend

1. Navigate to the folder "Frontend".
2. Run "python3 frontend.py".
3. The script will periodically pull all of the data from the AggregateTable in DynamoDB.

## Running the Generator

1. Navigate to the folder "Generator"
2. Before you create any data and ingest it into the pipeline, you may want to reset the data in the DynamoDB tables. To do this, simply run: "python3 clearTables.py"
3. Now - make sure that the number of messages is specified appropriately - then, run: "python3 generator.py".

## Updating the Lambda Functions
1. cd ./deployment
2. chmod +x ./dependencies.sh
3. ./dependencies.sh
3. ./../updateLambdas.sh

## Measurements
Currently there is a single EC2 that runs influxDB and grafana, the instance is on elastic IP which is hardcoded in all lambdas. Login to grafana via http://18.197.209.72:3000/ using admin/htcadmin and navigate to MapReduceDashboard. It is possible that you need to disconnect from VPN because of the port 3000.