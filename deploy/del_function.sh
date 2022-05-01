#!/bin/bash

BASE=`pwd`/`dirname $0`
DEPLOY_PREFIX="CacheNode"
DEPLOY_FROM=0
DEPLOY_CLUSTER=400
ARG_PROMPT="[num_functions]"

EMPH="\033[1;33m"
RESET="\033[0m"

source $BASE/arg_parser.sh

if [ ! $DEPLOY_TO ] && [ $1 ] ; then
  DEPLOY_CLUSTER=$1
fi
if [ ! $DEPLOY_TO ] ; then
  DEPLOY_TO=$((DEPLOY_CLUSTER-1))
fi

echo -e "Deleting Lambda "$EMPH"deployments"$RESET" ${DEPLOY_PREFIX}${DEPLOY_FROM} to ${DEPLOY_PREFIX}${DEPLOY_TO} ..."
read -p "Press any key to confirm, or ctrl-C to stop."
for ((i = ${DEPLOY_FROM}; i <= ${DEPLOY_TO}; i++)); do
  aws lambda delete-function --function-name ${DEPLOY_PREFIX}$i
done



