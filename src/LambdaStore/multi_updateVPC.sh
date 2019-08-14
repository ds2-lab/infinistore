#!/bin/bash

PREFIX="Store1VPCNode"
SUBNETS="subnet-b53a6bff,subnet-fcde0bc2"
SGS="sg-079f6cc4e658209c3"

echo "updating lambda code.."

for i in {0..64}
do
	aws lambda update-function-configuration --function-name $PREFIX$i --vpc-config SubnetIds=$SUBNETS,SecurityGroupIds=$SGS
done
