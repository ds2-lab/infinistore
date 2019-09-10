#!/bin/bash
LAMBDA="/aws/lambda/Store1VPCNode"
FILE="lambda/"

PREFIX=$1
start=$2
end=$3

# Convert date into seconds (Format is %s)
startTime=$(date  -j -f "%Y-%m-%d %H:%M:%S" "$start" +%s)000
endTime=$(date  -j -f "%Y-%m-%d %H:%M:%S" "$end" +%s)000


for i in {0..399}
do
    aws logs create-export-task --log-group-name $LAMBDA$i --from ${startTime} --to ${endTime} --destination "tianium.default" --destination-prefix $FILE$PREFIX$i
    sleep 2s
done
