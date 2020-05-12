#!/bin/bash

PWD=`dirname $0`
BASE=`pwd`/`dirname $0`
PROXY=$BASE/../bin/proxy
REDBENCH=$BASE/../bin/redbench
DEPLOY=$BASE/../../deploy

function reclaim_lambda() {
    NAME=$1
    RECLAIM=$2
    TIMEOUT=$3
    go run $DEPLOY/deploy_function.go -config -prefix=$NAME -vpc -from=$RECLAIM -to=$((RECLAIM+1)) -timeout=$TIMEOUT
    echo "Node $NAME$RECLAIM reclaimed."
}

function update_lambda_mem() {
    NAME=$1
    CLUSTER=$2
    MEM=$3
    TIMEOUT=$4
    echo "updating all nodes to memory of $MEM"
    go run $DEPLOY/deploy_function.go -config -prefix=$NAME -vpc -to=$CLUSTER -mem=$MEM -timeout=$TIMEOUT
}

function start_proxy() {
    echo "starting proxy server"
    PREFIX=$1
    BACKUPS=$2
    $PROXY -disable-dashboard -enable-evaluation -prefix=$PREFIX -numbak=$BACKUPS # -debug
}

function bench() {
    N=$1
    C=$2
    KEYMIN=$3
    KEYMAX=$4
    SZ=$5
    OP=$6
    INTERVAL=$7
    $REDBENCH -addrlist localhost:6378 -n $N -c $C -keymin $KEYMIN -keymax $KEYMAX \
    -sz $SZ -cli=redis -op $OP -i INTERVAL
}
