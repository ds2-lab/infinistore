#!/bin/bash

prefix=$1

for i in {0..64}
do
  aws lambda update-function-configuration --function-name $prefix$name$i --timeout $2
done
