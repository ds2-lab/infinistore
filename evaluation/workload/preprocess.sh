#!/bin/bash

LAMBDAPREFIX="Function Prefix"

PREFIX=$1
if [ "$PREFIX" == "" ] ; then
  echo "Manual: preprocess prefix"
  exit
fi

BASE=`pwd`/`dirname $0`
BIN=$BASE/../bin
DATA=$BASE/../downloaded
OUTPUT=$DATA/$PREFIX
CLUSTER=1000

mkdir -p $OUTPUT

cd $BASE/..

# Unzip
mkdir -p $DATA/proxy/$PREFIX
tar -xzf $DATA/proxy/$PREFIX.tar.gz -C $DATA/proxy/$PREFIX/

# Decode .clog file
./infla.sh $PREFIX

# Extract cluster data from proxy output
cat $OUTPUT/simulate-${CLUSTER}_proxy.csv | grep cluster, > $OUTPUT/cluster.csv
cat $OUTPUT/simulate-${CLUSTER}_proxy.csv | grep bucket, > $OUTPUT/bucket.csv

# Extract billing info from cloudwatch log
cloudwatch/parse.sh $DATA/log/$PREFIX
cat $DATA/log/${PREFIX}_bill.csv | grep invocation > $OUTPUT/bill.csv
make build-data
$BIN/preprocess -o $OUTPUT/recovery.csv -processor workload -fprefix $LAMBDAPREFIX $DATA/data/$PREFIX