#!/bin/bash

source ./settings.sh

${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper $ZKSTRING | grep ${USER}
