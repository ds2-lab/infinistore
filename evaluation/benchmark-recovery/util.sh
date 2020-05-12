#!/bin/bash

PWD=`dirname $0`
BASE=`pwd`/`dirname $0`
EVALBASE=$BASE/..
PROJECTBASE=$BASE/../..
BINDIR=$EVALBASE/bin
DEPLOY=$PROJECTBASE/deploy

function reclaim_lambda() {
    # NAME=$1
    RL_RECLAIM=$2
    # MEM=$3
    # TIMEOUT=$4
    go run $DEPLOY/deploy_function.go -config -prefix=$1 -vpc -from=$2 -to=$((RL_RECLAIM+1)) -mem=$3 -timeout=$4
    echo "Node $NAME$RECLAIM reclaimed."
}

function update_lambda_mem() {
    # NAME=$1
    # CLUSTER=$2
    # MEM=$3
    # TIMEOUT=$4
    echo "updating all nodes to memory of $MEM"
    go run $DEPLOY/deploy_function.go -config -prefix=$1 -vpc -to=$2 -mem=$3 -timeout=$4
}

function start_proxy() {
    echo "starting proxy server"
    # PREFIX=$1
    # BACKUPS=$2
    LOGFILE=`dirname $1`
    mkdir -p $EVALBASE/$LOGFILE
    $BINDIR/proxy -debug -disable-dashboard -enable-evaluation -base=$EVALBASE -log $LOGFILE.log -prefix=$1 -numbak=$2 # -debug
}

function bench() {
    # N=$1
    # C=$2
    # KEYMIN=$3
    # KEYMAX=$4
    # SZ=$5
    # OP=$6
    # INTERVAL=$7
    $BINDIR/redbench -addrlist localhost:6378 -n $1 -c $2 -keymin $3 -keymax $4 \
      -sz $5 -cli=redis -op $6 -i $7
}
