#!/bin/bash

PREFIX=$1
BASE=`pwd`/`dirname $0`

DATA=$BASE/../downloaded/data/$PREFIX
mkdir -p $DATA

aws s3 cp s3://tianium.default/data/$PREFIX $DATA --recursive
