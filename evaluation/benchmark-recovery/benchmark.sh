#!/bin/bash

if [ "$GOPATH" == "" ] ; then
  echo "No \$GOPATH defined. Install go and set \$GOPATH first."
fi

PWD=`dirname $0`
BASE=`pwd`/$PWD
ENTRY=`date "+%Y%m%d%H%M"`
ENTRY="data/$ENTRY"
NODE_PREFIX="Your Lambda Function Prefix"
MINBACKUPS=10
OVERHEAD=100
CLUSTER=12
TIMEOUTBASE=700

source $PWD/util.sh

function perform(){
    SECS=$1
    C=$2
    KEYS=$3
    SZ=$4
    INTERVAL=$5
    MEM=$6
    BACKUPS=$7

    ((NODES=CLUSTER+BACKUPS))
    ((BYTES=SZ*1024*1024))
    ((BAKOVERHEAD=MEM/MINBACKUPS))   # Reserved.
    ((SETS=(MEM-OVERHEAD)*10/SZ))  # Default EC configuration: 10+2
    ((N=SECS*1000/INTERVAL))
    RECLAIM=0

    for i in {1..5}
    do
        PREPROXY=$ENTRY/No.$i"_"lambda$MEM"_"$BACKUPS"_"$SZ"_"$INTERVAL

        update_lambda_mem $NODE_PREFIX $NODES $MEM $((TIMEOUTBASE+i*10))
        # Wait for proxy ready
        start_proxy $PREPROXY $BACKUPS &
        while [ ! -f /tmp/infinicache.pid ]
        do
            sleep 1s
        done
        echo pid:`cat /tmp/infinicache.pid`

        # Set objects
        sleep 1s
        echo "Setting $SETS $SZMB objects, $((SETS*SZ))MB in total."
        bench $SETS 1 1 $SETS $BYTES 0 0

        echo "Wait 60s for persisting..."
        sleep 60s

        # Trigger force reclaimation, the lambda 0 will be reclaimed in the middle of experiment.
        echo "Trigger reclaimation of node $RECLAIM in 5 seconds"
        /bin/bash -c "$PWD/reclaim.sh $NODE_PREFIX $RECLAIM $MEM 5 $i &"

        # Get objects, be sure to long enough
        echo "Getting random $N objects for $SECS seconds, interval: $INTERVAL ms"
        bench $N 1 1 $SETS $BYTES 1 $INTERVAL
        kill -2 `cat /tmp/infinicache.pid`
    done
}

#perform $*
#perform

# Seconds to run.
LASTING=(60)
# Memory settings
MEMSET=(512 1024 1536 2048 3008)
# Object size settings
SZSET=(1 10 50 100)
# Inter-arrival time settings
IASET=(200 500 1000 2000)
# # of backup nodes settings
BAKSET=(10 20 40 80)
CONCURRENCY=1
MAXKEY=1          # Occupant
if [ "$1" != "" ]; then
    TIMEOUT="$1"
fi

mkdir -p $PWD/$ENTRY
for mem in {0..4}
do
    for sz in {0..3}
    do
      iaIdx=1
      bak=0
      #       seconds       concur       keys    object-size  inter-arrival   memory       num-backups
      perform ${LASTING[0]} $CONCURRENCY $MAXKEY ${SZSET[sz]} ${IASET[iaIdx]} ${MEMSET[mem]} ${BAKSET[bak]}
    done
done
