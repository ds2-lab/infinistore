#!/bin/bash

prefix="Store1"
name="Node"

GOOS=linux go get
GOOS=linux go build redeo_lambda.go
zip LambdaStore redeo_lambda

echo "Creating lambda functions..."

for i in {0..35}
do
	aws lambda create-function \
	--function-name $prefix$name$i \
	--runtime go1.x \
	--role arn:aws:iam::022127035044:role/lambda-store \
	--handler redeo_lambda \
	--zip-file fileb://LambdaStore.zip

done
go clean
