#!/bin/bash

# Run `./create_function.sh -h` for available options.

BASE=`pwd`/`dirname $0`
DEPLOY_PREFIX="MemoryNode"
KEY="lambda"
DEPLOY_FROM=0
DEPLOY_CLUSTER=1000
DEPLOY_TO=$((DEPLOY_CLUSTER-1))
DEPLOY_MEM=1536
DEPLOY_VPC="-vpc"
ARG_PROMPT="timeout"
EXPECTING_ARGS=1

S3="ds2-lab.infinistore"
EMPH="\033[1;33m"
RESET="\033[0m"

# Parse arguments
source $BASE/arg_parser.sh

TIMEOUT=$1
if [ -z "$TIMEOUT" ]; then
  echo "No timeout specified, please specify a timeout in seconds."
  exit 1
fi

echo -e "Creating Lambda "$EMPH"deployments"$RESET" ${DEPLOY_PREFIX}${DEPLOY_FROM} to ${DEPLOY_PREFIX}${DEPLOY_TO} of $DEPLOY_MEM MB, ${TIMEOUT}s timeout..."
read -p "Press any key to confirm, or ctrl-C to stop."

cd $BASE/../lambda
echo "Compiling lambda code..."
GOOS=linux GOARCH=amd64 go build
echo "Compressing file..."
zip $KEY $KEY
echo "Putting code zip to s3"
aws s3api put-object --bucket ${S3} --key $KEY.zip --body $KEY.zip

echo "Creating Lambda deployments..."
go run $BASE/deploy_function.go -yaml $BASE/config.yml -s3-bucket-lambda ${S3} -create -prefix=$DEPLOY_PREFIX $DEPLOY_VPC -key=$KEY -from=$DEPLOY_FROM -to=${DEPLOY_CLUSTER} -mem=$DEPLOY_MEM -timeout=$TIMEOUT
rm $KEY*
