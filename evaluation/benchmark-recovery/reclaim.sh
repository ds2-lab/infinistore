#!/bin/bash

if [ "$GOPATH" == "" ] ; then
  echo "No \$GOPATH defined. Install go and set \$GOPATH first."
fi

PWD=`dirname $0`

NODE_PREFIX=$1
RECLAIM=$2
WAIT="${3}s"
I=$4

((TIMEOUT=900-i*10))

source $PWD/util.sh

echo "Wait $WAIT"
sleep $WAIT

reclaim_lambda $NODE_PREFIX $RECLAIM $TIMEOUT
