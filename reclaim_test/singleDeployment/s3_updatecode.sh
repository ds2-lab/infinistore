#!/bin/bash

BASE=`pwd`/`dirname $0`
PREFIX=$1
ROLE=$2
BUCKET=$3
KEY="reclaim"
mem=3008

echo "compiling lambda code..."
GOOS=linux go build -o $KEY ./main_$KEY
echo "compress file..."
zip $KEY $KEY
echo "updating lambda code.."

echo "putting code zip to s3"
aws s3api put-object --bucket tianium.default --key $KEY.zip --body $KEY.zip

go run $BASE/../../deploy/deploy_function.go -from=0 -to=300 -role=$ROLE -bucket=$BUCKET -prefix=$PREFIX \
-create=true -config=true -vpc=false -key=$KEY -mem=$mem -timeout=200

go clean
rm $KEY.zip
