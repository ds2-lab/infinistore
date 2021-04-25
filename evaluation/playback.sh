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
	PROXY_PARAMS=$3
	PLAY_PARAMS=$4
	COMPACT=$5
	DASHBOARD=$6

	if [ "$COMPACT" == "-enable-dashboard" ] ; then
		DASHBOARD=$COMPACT
		COMPACT=
	fi

	PREPROXY=$PWD/$ENTRY/simulate-$CLUSTER$COMPACT

	start_proxy $PREPROXY "$DASHBOARD" "$PROXY_PARAMS"
  # Wait for proxy is ready
	while [ ! -f /tmp/infinicache.pid ]
	do
		sleep 1s
	done
	cat /tmp/infinicache.pid
	# playback
	sleep 1s
	playback "-cluster=$CLUSTER -file=$PREPROXY $COMPACT $PLAY_PARAMS" $FILE
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
	PARAMS=$3

	dryrun "-cluster=$CLUSTER $PARAMS" $FILE
}

if [ "$1" == "dryrun" ]; then
	dry_perform $2 $3 "$4"
else
	mkdir -p $PWD/$ENTRY
	CLUSTER=$2
	((MAXLAMBDAID=CLUSTER-1))

	START=`date +"%Y-%m-%d %H:%M:%S"`
	perform $1 $2 "$3" "$4" $5 $6
	mv $PWD/log $PWD/$ENTRY.log
	END=`date +"%Y-%m-%d %H:%M:%S"`
	
	echo "Transfering logs from CloudWatch to S3: [cloudwatch/export_ubuntu.sh $DATE/ \"$START\" \"$END\" 0 $MAXLAMBDAID] ..."
	cloudwatch/export_ubuntu.sh $DATE/ "$START" "$END" 0 $MAXLAMBDAID
fi
