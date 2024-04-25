#!/bin/bash

echo "Starting Zookeeper..."
# Assuming the default installation path for Kafka and Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 5

echo "Starting Kafka Server..."
bin/kafka-server-start.sh config/server.properties &
sleep 10

echo "Starting MongoDB..."
# Assuming MongoDB is installed and the `mongod` command is available in PATH
mongod --dbpath /path/to/your/mongodb/data/folder &
sleep 5

echo "Running Kafka Producer..."
# Assuming your producer script is a Python script
python3 /path/to/your/producer.py &

echo "Running Kafka Consumer..."
# Assuming your consumer script is a Python script
python3 /path/to/your/consumer.py &

# Optional: add any other services or scripts you need to run

echo "All services started. Check logs for details."


# How to run this script?
# Create a new file named start_services.sh and open it in a text editor
# nano start_services.sh

# Make your script executable by running:
# chmod +x start_services.sh

# Run your script using:
# ./start_services.sh

# If you're using a virtual environment for Python, you may need to activate it in the script before running the Python commands.
# source /path/to/virtualenv/bin/activate
