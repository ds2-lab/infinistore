#!/bin/bash

if [ "$GOPATH" == "" ] ; then
  echo "No \$GOPATH defined. Install go and set \$GOPATH first."
fi

PWD=`dirname $0`
BASE=`pwd`/$PWD
ENTRY=`date "+%Y%m%d%H%M"`
ENTRY="/data/$ENTRY"
NODE_PREFIX="Your Lambda Function Prefix"
MINBACKUPS=10
OVERHEAD=100
CLUSTER=12

source $PWD/util.sh

function perform(){
    SECS=$1
    C=$2
    KEYS=$3
    SZ=$4
    INTERVAL=$5
    MEM=$6
    BACKUPS=$7
    TIMEOUT=$8

    ((NODES=CLUSTER+BACKUPS))
    ((BAKOVERHEAD=MEM/MINBACKUPS))   # Reserved.
    ((SETS=(MEM-OVERHEAD)/(SZ/10)))  # Default EC configuration: 10+2
    ((N=SECS*1000/INTERVAL))
    RECLAIM=0

    for i in {1..5}
    do
        PREPROXY=$PWD/$ENTRY/No.$i"_"lambda$MEM"_"$BACKUPS"_"$SZ"_"$INTERVAL

        update_lambda_mem $NODE_PREFIX $NODES $MEM $((TIMEOUT+i*10))
        # Wait for proxy ready
        start_proxy $PREPROXY $BACKUPS &
        while [ ! -f /tmp/infinicache.pid ]
        do
            sleep 1s
        done
        cat /tmp/infinicache.pid

        # Set objects
        sleep 1s
        echo "Setting $((MEM-OVERHEAD-BAKOVERHEAD))MB objects"
        bench $SETS 1 1 $SETS $SZ 0 0

        echo "Wait 60s for persisting..."
        sleep 60s

        # Trigger force reclaimation, the lambda 0 will be reclaimed in the middle of experiment.
        echo "Trigger reclaimation of node $RECLAIM in 5 seconds"
        /bin/bash -c "$PWD/reclaim.sh $NODE_PREFIX $RECLAIM 5 $i &"

        # Get objects, be sure to long enough
        bench $N 1 1 $SETS $SZ 1 $INTERVAL
        kill -2 `cat /tmp/infinicache.pid`
    done
}

#perform $*
#perform

SEC=(60)
MEMSET=(512 1024 1536 2048 3008)
SZSET=(1048576 10485760 52428800 104857600)
INTERVAL=(200 500 1000 2000)
BACKUPS=(10 20 40 80)
CONCURRENCY=1
KEYS=1          # Occupant
TIMEOUT=600
if [ "$1" != "" ]; then
    TIMEOUT="$1"
fi

mkdir -p $PWD/$ENTRY
for mem in {0..4}
do
    for sz in {0..3}
    do
      i=0
      bak=1
      # perform seconds concur       keys  object-size  inter-arrival  memory         num-backups     timeout
      perform ${SEC[i]} $CONCURRENCY $KEYS ${SZSET[sz]} ${INTERVAL[i]} ${MEMSET[mem]} ${BACKUPS[bak]} $TIMEOUT
    done
done

mv $PWD/log $PWD/$ENTRY.log
