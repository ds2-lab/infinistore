#!/bin/bash

BASE=`pwd`/`dirname $0`
PREFIX="Store1VPCNode"
KEY="lambda"
start=0
cluster=400
mem=1536
# try -code

S3="tianium.default"
EMPH="\033[1;33m"
RESET="\033[0m"

if [ "$2" == "" ] ; then
    CODE=""
elif [ "$2" == "-code" ] ; then
    CODE="$2"
else
    CODE="$3"
    PREFIX="$2"
fi

if [ "$CODE" == "-code" ] ; then
    echo -e "Updating "$EMPH"code and configuration"$RESET" of Lambda deployments ${PREFIX}${start} to ${PREFIX}$((start+cluster-1)) to $mem MB, $1s timeout..."
    read -p "Press any key to confirm, or ctrl-C to stop."

    cd $BASE/../lambda
    echo "Compiling lambda code..."
    GOOS=linux go build
    echo "Compressing file..."
    zip $KEY $KEY
    echo "Putting code zip to s3"
    aws s3api put-object --bucket ${S3} --key $KEY.zip --body $KEY.zip
else 
    echo -e "Updating "$EMPH"configuration"$RESET" of Lambda deployments ${PREFIX}${start} to ${PREFIX}$((start+cluster)) to $mem MB, $1s timeout..."
    read -p "Press any key to confirm, or ctrl-C to stop."
fi

echo "Updating Lambda deployments..."
go run $BASE/deploy_function.go -S3 ${S3} $CODE -config -prefix=$PREFIX -vpc -key=$KEY -from=$start -to=$((start+cluster)) -mem=$mem -timeout=$1

if [ "$CODE" == "-code" ] ; then
  rm $KEY*
fi
