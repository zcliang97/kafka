#!/bin/bash

source ./settings.sh

echo --- Cleaning
rm -f Application.class

echo --- Compiling Java
$JAVA_CC Application.java
if [ $? -eq 0 ]
then
  echo "Success..."
else
  echo "Error..."
  exit 1
fi

echo --- Running

$JAVA Application $KBROKERS $APP_NAME $STOPIC $CTOPIC $OTOPIC $STATE_STORE_DIR
