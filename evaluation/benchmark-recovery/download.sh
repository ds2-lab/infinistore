#!/bin/bash

PREFIX=$1
BASE=`pwd`/`dirname $0`

source $BASE/../config.mk

DATA=$BASE/../downloaded/data/$PREFIX
mkdir -p $DATA

aws s3 cp s3://$S3_BUCKET_DATA/data/$PREFIX $DATA --recursive
