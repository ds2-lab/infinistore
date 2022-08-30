#!/bin/bash

if [ "$GOPATH" == "" ] ; then
	echo "No \$GOPATH defined. Install go and set \$GOPATH first."
fi

PWD=`dirname $0`
DATE=`date "+%Y%m%d%H%M"`
ENTRY="data/$DATE"
NODE_PREFIX="Store1VPCNode"
PID=/tmp/infinicache.pid

source $PWD/util.sh
mkdir -p "$PWD/$ENTRY"

function perform(){
	CMD=$1
	FILE=$2
	CLUSTER=$3
	PROXY_PARAMS=$4
	PLAY_PARAMS=$5
	COMPACT=$6
	DASHBOARD=$7

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
	sleep 1s

	# playback
	PLAYBACK_RET=1
	RETRIAL=0
	while [ $PLAYBACK_RET -ne 0 -a $RETRIAL -lt 10 ]	# limit to 10 retrials
	do
		if [ "$CMD" == "exec" ] ; then
			$FILE $PLAY_PARAMS --prefix ${PREPROXY}
			PLAYBACK_RET=0 # exec mode does not support checkpoint
		else
			((RETRIAL=RETRIAL+1)) # Update retrial for different runs to avoid the collision of output data.
			playback "-cluster=$CLUSTER -file=$PREPROXY-$RETRIAL $COMPACT $PLAY_PARAMS -checkpoint=$ENTRY.checkpoint" $FILE
			PLAYBACK_RET=$?
			echo "playback return $PLAYBACK_RET"
		fi
	done
	
	# stop proxy
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
	FILE=$2
	CLUSTER=$3
	PROXY_PARAMS=$4
	PLAY_PARAMS=$5
	COMPACT=$6
	DASHBOARD=$7

	# dryrun "-cluster=$CLUSTER $PARAMS" $FILE

	PREPROXY=$PWD/$ENTRY/simulate-$CLUSTER$COMPACT
	echo "$FILE $PLAY_PARAMS --prefix ${PREPROXY}"
	$FILE $PLAY_PARAMS --prefix ${PREPROXY}
}

function custom(){
	FILE=$2
	CLUSTER=$3
	PLAY_PARAMS=$4

	PREPROXY=$PWD/$ENTRY/simulate-$CLUSTER

	# playback
	PLAYBACK_RET=1
	RETRIAL=0
	while [ $PLAYBACK_RET -ne 0 -a $RETRIAL -lt 10 ]	# limit to 10 retrials
	do
		((RETRIAL=RETRIAL+1)) # Update retrial for different runs to avoid the collision of output data.
		playback "-file=$PREPROXY-$RETRIAL $PLAY_PARAMS -checkpoint=$ENTRY.checkpoint" $FILE
		PLAYBACK_RET=$?
		echo "playback return $PLAYBACK_RET"
	done
}

if [ "$1" == "dryrun" ]; then
	echo "Dry run"
	dry_perform "$1" "$2" "$3" "$4" "$5" "$6" "$7"
elif [ "$1" == "custom" ]; then
	echo "Custom"
	custom "$1" "$2" "$3" "$4"
	mv $PWD/log $PWD/$ENTRY.log
else
	mkdir -p $PWD/$ENTRY
	CLUSTER=$3
	((MAXLAMBDAID=CLUSTER-1))

	START=`date +"%Y-%m-%d %H:%M:%S"`
	perform "$1" "$2" "$3" "$4" "$5" "$6" "$7"
	mv $PWD/log $PWD/$ENTRY.log
	END=`date +"%Y-%m-%d %H:%M:%S"`
	
	echo "Transfering logs from CloudWatch to S3: [cloudwatch/export_ubuntu.sh $DATE/ \"$START\" \"$END\" 0 $MAXLAMBDAID] ..."
	cloudwatch/export_ubuntu.sh $DATE/ "$START" "$END" 0 $MAXLAMBDAID
fi
