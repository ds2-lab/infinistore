#!/bin/bash

BASE=`pwd`/`dirname $0`
BIN=$BASE/../bin
DATA=$BASE/../downloaded
OUTPUT=$DATA/ycsb

mkdir -p $OUTPUT

cd $BASE/..

make build-data
$BIN/preprocess -o $OUTPUT/ycsb.csv -processor ycsb -yaml $BASE/preprocess.yml $DATA/data_redis
$BIN/preprocess -a -o $OUTPUT/ycsb.csv -processor ycsb -yaml $BASE/preprocess.yml $DATA/data_redisc
$BIN/preprocess -a -o $OUTPUT/ycsb.csv -processor ycsb -yaml $BASE/preprocess.yml $DATA/data_sion
$BIN/preprocess -a -o $OUTPUT/ycsb.csv -processor ycsb -yaml $BASE/preprocess.yml $DATA/data_anna