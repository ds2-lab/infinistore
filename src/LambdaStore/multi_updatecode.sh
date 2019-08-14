#!/bin/bash

PREFIX="Store1VPCNode"
if [ "$1" != "" ] ; then
  PREFIX=$1
fi
mem=1024
# concurrency=30

echo "compiling lambda code..."
GOOS=linux go build redeo_lambda.go
echo "compress file..."
zip LambdaStore redeo_lambda
echo "updating lambda code.."

for i in {0..64}
do
     aws lambda update-function-code --function-name $PREFIX$i --zip-file fileb://LambdaStore.zip
     aws lambda update-function-configuration --function-name $PREFIX$i --memory-size $mem --timeout $2
     # aws lambda update-function-configuration --function-name $PREFIX$i --timeout $2
#    aws lambda update-function-configuration --function-name $PREFIX$i --handler redeo_lambda
#    aws lambda put-function-concurrency --function-name $PREFIX$i --reserved-concurrent-executions $concurrency
done

go clean
