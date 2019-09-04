#!/bin/bash

PREFIX="Store1VPCNode"
mem=1024

echo "compiling lambda code..."
GOOS=linux go build redeo_lambda.go
echo "compress file..."
zip LambdaStore redeo_lambda
echo "updating lambda code.."

echo "putting code zip to s3"
aws s3api put-object --bucket tianium.default --key lambdastore.zip --body LambdaStore.zip

for i in {0..299}
do
     # aws lambda update-function-code --function-name $PREFIX$i --s3-bucket tianium.default --s3-key lambdastore.zip
     if [ "$1" != "" ] ; then
       aws lambda update-function-configuration --function-name $PREFIX$i --memory-size $mem --timeout $1
     fi
     # aws lambda update-function-configuration --function-name $PREFIX$i --timeout $2
#    aws lambda update-function-configuration --function-name $PREFIX$i --handler redeo_lambda
#    aws lambda put-function-concurrency --function-name $PREFIX$i --reserved-concurrent-executions $concurrency
done

go clean
