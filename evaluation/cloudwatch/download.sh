#!/bin/bash

PREFIX=$1
TYPE=$2
PWD=`dirname $0`

BASE=$PWD/../downloaded/$TYPE/$PREFIX
mkdir -p BASE

aws s3 cp s3://jzhang33.default/$TYPE/$PREFIX $BASE --recursive
