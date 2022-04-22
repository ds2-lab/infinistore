#!/bin/bash

BASE=`pwd`/`dirname $0`
DEPLOY_PREFIX="ElasticMB"
KEY="lambda"
NUM_DEPLOYS=9
ARG_PROMPT="timeout"
EXPECTING_ARGS=1
# try -code

S3="mason-leap-lab.infinicache"
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
    echo -e "Updating "$EMPH"code and configuration"$RESET" of Lambda deployments ${DEPLOY_PREFIX}0- to ${DEPLOY_PREFIX}$NUM_DEPLOYS- to ${TIMEOUT}s timeout..."
    read -p "Press any key to confirm, or ctrl-C to stop."

    cd $BASE/../lambda
    echo "Compiling lambda code..."
    GOOS=linux go build
    echo "Compressing file..."
    zip $KEY $KEY
    echo "Putting code zip to s3"
    aws s3api put-object --bucket ${S3} --key $KEY.zip --body $KEY.zip
else 
    echo -e "Updating "$EMPH"configuration"$RESET" of Lambda deployments ${DEPLOY_PREFIX}0- to ${DEPLOY_PREFIX}$NUM_DEPLOYS- to ${TIMEOUT}s timeout..."
    read -p "Press any key to confirm, or ctrl-C to stop."
fi

echo "Updating Lambda deployments..."
for i in $(seq 0 1 $NUM_DEPLOYS)
do
  echo "update_function.sh --prefix=$DEPLOY_PREFIX$i- $CODE --no-build $TIMEOUT"
  $BASE/update_function.sh --prefix=$DEPLOY_PREFIX$i- $CODE --no-build $TIMEOUT
done

if [ "$CODE" == "-code" ] ; then
  rm $KEY*
fi

