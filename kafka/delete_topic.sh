#!/bin/bash

source ./settings.sh

${KAFKA_HOME}/bin/kafka-topics.sh --delete --zookeeper $ZKSTRING --topic $1
