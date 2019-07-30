#!/bin/bash

name="Lambda2SmallJPG"
mem=3008
concurrency=30

echo "compiling lambda code..."
GOOS=linux go build redeo_lambda.go
echo "compress file..."
zip Lambda2SmallJPG redeo_lambda
echo "updating lambda code.."
# aws lambda update-function-code --function-name $name --zip-file fileb://Lambda2SmallJPG.zip
aws lambda update-function-configuration --function-name $name --memory-size $mem --timeout 555
# aws lambda put-function-concurrency --function-name $name --reserved-concurrent-executions $concurrency

go clean