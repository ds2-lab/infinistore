#!/bin/bash

if [ "$GOPATH" == "" ] ; then
	echo "No \$GOPATH defined. Install go and set \$GOPATH first."
fi

PWD=`dirname $0`
DATE=`date "+%Y%m%d%H%M"`
ENTRY="/data/$DATE"
NODE_PREFIX="Store1VPCNode"
PID=/tmp/infinicache.pid

source $PWD/util.sh

function perform(){
	FILE=$1
	CLUSTER=$2
	DATANUM=$3
	PARITYNUM=$4
	SCALE=$5
	COMPACT=$6
	DASHBOARD=$7

	if [ "$COMPACT" == "-enable-dashboard" ] ; then
		DASHBOARD=$COMPACT
		COMPACT=
	fi

	PREPROXY=$PWD/$ENTRY/simulate-$CLUSTER$COMPACT

	start_proxy $PREPROXY $DASHBOARD
  # Wait for proxy is ready
	while [ ! -f /tmp/infinicache.pid ]
	do
		sleep 1s
	done
	cat /tmp/infinicache.pid
	# playback
	sleep 1s
	playback $DATANUM $PARITYNUM $SCALE $CLUSTER $FILE $PREPROXY $COMPACT $OUTPUT
	kill -2 `cat $PID`
  # Wait for proxy cleaned up
	TIMEOUT=60
  while [ -f /tmp/infinicache.pid ]
	do
		sleep 1s
		((TIMEOUT=TIMEOUT-1))
		if [ "$TIMEOUT" == "0" ] ; then
			# Force kill
			kill -9 `cat $PID`
			rm $PID
			break
		fi
	done

	if [ -f $PWD/proxy.log ] ; then
		mv $PWD/proxy.log $PWD/${ENTRY}_proxy.log
	fi
}

function dry_perform(){
	FILE=$1
	CLUSTER=$2
	DATANUM=$3
	PARITYNUM=$4
	SCALE=$5
	COMPACT=$6

	dryrun $DATANUM $PARITYNUM $SCALE $CLUSTER $FILE dryrun $COMPACT
}

if [ "$7" == "dryrun" ]; then
	dry_perform $1 $2 $3 $4 $5 $6
else
	mkdir -p $PWD/$ENTRY

	START=`date +"%Y-%m-%d %H:%M:%S"`
	perform $1 $2 $3 $4 $5 $6 $7
	mv $PWD/log $PWD/$ENTRY.log
	END=`date +"%Y-%m-%d %H:%M:%S"`

	
	echo "Transfering logs from CloudWatch to S3: $START - $END ..."
	cloudwatch/export_ubuntu.sh $DATE/ "$START" "$END"
fi
