#!/bin/bash

prefix="Store1VPCNode"
if [ "$1" != "" ] ; then
  prefix=$1
fi
mem=3008
# concurrency=30

echo "compiling lambda code..."
GOOS=linux go build redeo_lambda.go
echo "compress file..."
zip LambdaStore redeo_lambda
echo "updating lambda code.."

for i in {0..13}
do
    aws lambda update-function-code --function-name $prefix$i --zip-file fileb://LambdaStore.zip
   # aws lambda update-function-configuration --function-name $prefix$i --memory-size $mem
   # aws lambda update-function-configuration --function-name $prefix$name$i --timeout $2
#    aws lambda update-function-configuration --function-name $prefix$name$i --handler redeo_lambda
#    aws lambda put-function-concurrency --function-name $name$i --reserved-concurrent-executions $concurrency
done

go clean
