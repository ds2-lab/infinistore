#!/bin/bash

PREFIX=$1
PWD=`dirname $0`

BASE=$PWD/../downloaded/data/$PREFIX
mkdir -p BASE

aws s3 cp s3://tianium.default/data/$PREFIX $BASE --recursive
