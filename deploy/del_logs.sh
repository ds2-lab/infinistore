#!/bin/bash

# deleting log in cloudWatch with prefix

BASE=`pwd`/`dirname $0`
DEPLOY_PREFIX="CacheNode"
DEPLOY_FROM=0
DEPLOY_CLUSTER=400
ARG_PROMPT="[num_logs]"

EMPH="\033[1;33m"
RESET="\033[0m"

source $BASE/arg_parser.sh

if [ ! $DEPLOY_TO ] && [ $1 ] ; then
  DEPLOY_CLUSTER=$1
fi
if [ ! $DEPLOY_TO ] ; then
  DEPLOY_TO=$((DEPLOY_CLUSTER-1))
fi

echo -e "Deleting "$EMPH"logs"$RESET" of Lambda deployments ${DEPLOY_PREFIX}${DEPLOY_FROM} to ${DEPLOY_PREFIX}${DEPLOY_TO} ..."
read -p "Press any key to confirm, or ctrl-C to stop."
for ((i = ${DEPLOY_FROM}; i <= ${DEPLOY_TO}; i++)); do
    echo -e "Deleting logs of Lambda deployment ${DEPLOY_PREFIX}$i ..."
    aws logs delete-log-group --log-group-name /aws/lambda/${DEPLOY_PREFIX}$i
done