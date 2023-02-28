#!/bin/bash


BASE=`pwd`/`dirname $0`
BIN=$BASE/../bin

# Configurable variables
LAMBDAPREFIX="MemoryNode"     # The prefix of the lambda functions as "DEPLOY_PREFIX" or "--prefix" on executing deploy/create_function.sh.
DATA=$BASE/../downloaded      # The directory where the data is stored.
OUTPUT=$DATA/$PREFIX
CLUSTER=1000                  # The number of Lambda functions that corresponds to individual experiment settings. This value is equal to "--cluster" on executing the proxy.

PREFIX=$1
if [ "$PREFIX" == "" ] ; then
  echo "Manual: $0 prefix [cluster]"
  exit
fi

if [ "$2" != "" ] ; then
  CLUSTER=$2
fi

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