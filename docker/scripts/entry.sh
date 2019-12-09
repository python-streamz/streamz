#!/bin/bash

# Activate the streamz-dev anaconda environment by default
source activate streamz-dev

# Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

# Configure Kafka
sed -i '/#listeners=PLAINTEXT:\/\/:9092/c\listeners=PLAINTEXT:\/\/localhost:9092' $KAFKA_HOME/config/server.properties
sed -i '/#advertised.listeners=PLAINTEXT:\/\/your.host.name:9092/c\advertised.listeners=PLAINTEXT:\/\/localhost:9092' $KAFKA_HOME/config/server.properties

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

# Start up a jupyter notebook
cd /streamz/examples && jupyter-lab --allow-root --ip=0.0.0.0 --no-browser --NotebookApp.token=''
