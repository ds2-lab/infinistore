#!/usr/bin/env bash

BASE=`pwd`/`dirname $0`
PREFIX=$1
EXPR=$BASE/downloaded/$1

if [ "$1" == "" ] ; then
	echo "Please specify the data directory, in the form of YYYYMMDDHHmm"
	exit 1
fi

mkdir -p $EXPR/

for dat in $BASE/downloaded/proxy/$PREFIX/$PREFIX/*.clog
do
    echo $dat
    NAME=$(basename $dat .clog)
    inflate -f $dat > $EXPR/${NAME}.csv
done