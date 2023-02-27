#!/bin/bash

PREFIX=$1
TYPE=$2
PWD=`dirname $0`

BASE=$PWD/../downloaded/$TYPE/$PREFIX
mkdir -p BASE

aws s3 cp s3://ds2-lab.datapool/$TYPE/$PREFIX $BASE --recursive
