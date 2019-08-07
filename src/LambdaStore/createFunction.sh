#!/bin/bash

PREFIX="Store1VPCNode"
SUBNETS="subnet-2b304b24,subnet-2b304b24"
SGS="sg-079f6cc4e658209c3"

GOOS=linux go get
GOOS=linux go build redeo_lambda.go
zip LambdaStore redeo_lambda

echo "Creating lambda functions..."

for i in {0..13}
do
	aws lambda create-function \
	--function-name $PREFIX$i \
	--runtime go1.x \
	--role arn:aws:iam::022127035044:role/lambda-store \
	--handler redeo_lambda \
	--zip-file fileb://LambdaStore.zip \
	--vpc-config SubnetIds=$SUBNETS,SecurityGroupIds=$SGS

done
go clean
