#!/bin/bash

PWD=`dirname $0`
BASE=`pwd`/`dirname $0`
EVALBASE=$BASE
PROJECTBASE=$BASE/..
BINDIR=$EVALBASE/bin

function update_lambda_timeout() {
    NAME=$1
    TIME=$2
    echo "updating lambda store timeout"
#    for i in {0..13}
    for i in {0..63}
    do
#            aws lambda update-function-code --function-name $prefix$i --zip-file fileb://Lambda2SmallJPG.zip
#            aws lambda update-function-configuration --function-name $prefix$i --memory-size $mem
        aws lambda update-function-configuration --function-name $NAME$i --timeout $TIME
#            aws lambda update-function-configuration --function-name $prefix$name$i --handler lambda
#            aws lambda put-function-concurrency --function-name $name$i --reserved-concurrent-executions $concurrency
    done
}

function update_lambda_mem() {
    NAME=$1
    MEM=$2
    echo "updating lambda store mem"
#    for i in {0..13}
    for i in {0..63}
    do
#            aws lambda update-function-code --function-name $prefix$i --zip-file fileb://Lambda2SmallJPG.zip
            aws lambda update-function-configuration --function-name $NAME$i --memory-size $MEM
#        aws lambda update-function-configuration --function-name $NAME$i --timeout $TIME
#            aws lambda update-function-configuration --function-name $prefix$name$i --handler lambda
#            aws lambda put-function-concurrency --function-name $name$i --reserved-concurrent-executions $concurrency
    done
}


function start_proxy() {
    echo "running proxy server"
    PREFIX=$1
    DASHBOARD=$2
    PARAMS=$3
    if [ "$DASHBOARD" != "" ] ; then
        echo "Run: GOMAXPROCS=36 $BINDIR/proxy -prefix=$PREFIX -lambda-prefix=\${LAMBDAPREFIX} -ip=\${PUBLICIP} -log=proxy.log $DASHBOARD $PARAMS"
    else
        GOMAXPROCS=36 $BINDIR/proxy -prefix=$PREFIX -lambda-prefix=${LAMBDAPREFIX} -ip=${PUBLICIP} $PARAMS &
    fi
}

function bench() {
    N=$1
    C=$2
    KEYMIN=$3
    KEYMAX=$4
    SZ=$5
    D=$6
    P=$7
    OP=$8
    FILE=$9
    $BINDIR/redbench -addrlist localhost:6378 -n $N -c $C -keymin $KEYMIN -keymax $KEYMAX \
    -sz $SZ -d $D -p $P -op $OP -file $FILE -dec -i 1000
}

function playback() {
    PARAMS=$1
    FILE=$2
    $BINDIR/playback -addrlist localhost:6378 $PARAMS $FILE
}

function dryrun() {
    PARAMS=$1
    FILE=$2
    $BINDIR/playback -dryrun $PARAMS $FILE
}
