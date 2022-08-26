#!/bin/bash

BASE=`pwd`/`dirname $0`
DEPLOY_PREFIX="Store1VPCNode"
KEY="lambda"
DEPLOY_FROM=0
DEPLOY_CLUSTER=1000
DEPLOY_TO=$((DEPLOY_CLUSTER-1))
DEPLOY_MEM=1024
ARG_PROMPT="timeout"
EXPECTING_ARGS=1

S3="jzhang33.default"
EMPH="\033[1;33m"
RESET="\033[0m"

# Parse arguments
source $BASE/arg_parser.sh

TIMEOUT=$1
if [ -z "$TIMEOUT" ]; then
  echo "No timeout specified, please specify a timeout in seconds."
  exit 1
fi

if [ "$CODE" == "-code" ] ; then
    echo -e "Updating "$EMPH"code and configuration"$RESET" of Lambda deployments ${DEPLOY_PREFIX}${DEPLOY_FROM} to ${DEPLOY_PREFIX}${DEPLOY_TO} to $DEPLOY_MEM MB, ${TIMEOUT}s timeout..."
    if [ ! $NO_BREAK ] ; then
        read -p "Press any key to confirm, or ctrl-C to stop."
    fi
    
    if [ ! $NO_BUILD ] ; then
        cd $BASE/../lambda
        echo "Compiling lambda code..."
        GOOS=linux go build
        echo "Compressing file..."
        zip $KEY $KEY
        echo "Putting code zip to s3"
        aws s3api put-object --bucket ${S3} --key $KEY.zip --body $KEY.zip
    fi

    if [ ! -e $KEY.zip ] ; then
        echo "No lambda binary found, exiting..."
        exit 1
    fi
else 
    echo -e "Updating "$EMPH"configuration"$RESET" of Lambda deployments ${DEPLOY_PREFIX}${DEPLOY_FROM} to ${DEPLOY_PREFIX}${DEPLOY_TO} to $DEPLOY_MEM MB, ${TIMEOUT}s timeout..."
    if [ ! $NO_BREAK ] ; then
        read -p "Press any key to confirm, or ctrl-C to stop."
    fi
fi

echo "Updating Lambda deployments..."
go run $BASE/deploy_function.go -S3 ${S3} $CODE -config -prefix=$DEPLOY_PREFIX -vpc -key=$KEY -from=$DEPLOY_FROM -to=${DEPLOY_CLUSTER} -mem=$DEPLOY_MEM -timeout=$TIMEOUT

if [ "$CODE" == "-code" ] && [ ! $NO_BUILD  ] ; then
  rm $KEY*
fi
