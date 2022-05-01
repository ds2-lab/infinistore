#!/bin/bash

BASE=`pwd`/`dirname $0`
source $BASE/arg_parser.sh

TIMEOUT=$1

if [ $VERBOSE ]; then
  echo "Update code: $CODE, timeout: $TIMEOUT"
  echo "Deploy prefix: $DEPLOY_PREFIX, mem: $DEPLOY_MEM, from: $DEPLOY_FROM, to: $DEPLOY_TO"
fi