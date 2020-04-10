#!/bin/bash

PWD=`dirname $0`
PREFIX="Store1VPCNode"
KEY="lambda"
cluster=400
mem=1536

S3="tianium.default"

echo "compiling lambda code..."
GOOS=linux go build $PWD/../lambda/$KEY.go
echo "compress file..."
zip $KEY $KEY
echo "updating lambda code.."

echo "putting code zip to s3"
aws s3api put-object --bucket ${S3} --key $KEY.zip --body $KEY.zip

go run $PWD/deploy_function.go -S3 ${S3} -code -config -prefix=$PREFIX -vpc -key=$KEY -to=$cluster -mem=$mem -timeout=$1
rm $KEY*
