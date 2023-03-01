#!/bin/bash

PREFIX=$1
TYPE=$2
PWD=`dirname $0`

source $PWD/../config.mk

BASE=$PWD/../downloaded/$TYPE/$PREFIX
mkdir -p BASE

aws s3 cp s3://$S3_BUCKET_DATA/$TYPE/$PREFIX $BASE --recursive
