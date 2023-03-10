#!/bin/bash

if [ "$GOPATH" == "" ] ; then
  echo "No \$GOPATH defined. Install go and set \$GOPATH first."
fi

PWD=`dirname $0`
ENTRY=`date "+%Y%m%d%H%M"`
ENTRY="/data/$ENTRY"

source $PWD/util.sh

function perform(){
    PREPROXY=$PWD/$ENTRY/dryrun-

    start_proxy $PREPROXY
}

mkdir -p $PWD/$ENTRY
perform
