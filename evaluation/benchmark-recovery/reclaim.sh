#!/bin/bash

if [ "$GOPATH" == "" ] ; then
  echo "No \$GOPATH defined. Install go and set \$GOPATH first."
fi

PWD=`dirname $0`

NODE_PREFIX=$1
RECLAIM=$2
MEM=$3
WAIT="${4}s"
I=$5

((TIMEOUT=900-I*10))

source $PWD/util.sh

echo "Wait $WAIT"
sleep $WAIT

reclaim_lambda $NODE_PREFIX $RECLAIM $MEM $TIMEOUT
