#!/bin/bash

source ./settings.sh

${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper $ZKSTRING --replication-factor 1 --partitions 1 --topic $1
