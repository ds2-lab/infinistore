#!/bin/bash

BASE=`pwd`/`dirname $0`
PROXY=$BASE/../bin/proxy
REDBENCH=$BASE/../bin/redbench
DEPLOY=$BASE/../../proxy

echo $PWD

function reclaim_lambda() {
    NAME=$1
    RECLAIM=$2
    ((RECLAIM_END=RECLAIM+1))
    TIMEOUT=$3
    echo "reclaiming node $RECLAIM"
    go run $DEPLOY/deploy_function.go -config -prefix=$NAME -vpc -from=$RECLAIM -to=$RECLAIM_END -timeout=$TIMEOUT
}

function update_lambda_mem() {
    NAME=$1
    CLUSTER=$2
    MEM=$3
    echo "updating all nodes to memory of $MEM"
    go run $DEPLOY/deploy_function.go -config -prefix=$NAME -vpc -to=$CLUSTER -mem=$MEM -timeout=600
}

function start_proxy() {
    echo "starting proxy server"
    PREFIX=$1
    $PROXY -prefix=$PREFIX -disable-dashboard # -debug
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
