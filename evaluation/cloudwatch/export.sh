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
  # Wait for the end the last task
  for j in {0..15}
  do
    RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
    if [ "$RUNNING" != "" ]; then
      sleep 2s
    else
      break
    fi
  done

  # Abandon
  if [ "$RUNNING" != "" ]; then
    echo "Detect running task and wait timeout, killing task \"$RUNNING\"..."
    aws logs --profile CWLExportUser cancel-export-task --task-id \"$RUNNING\"
    echo "Done"
  fi

  # Execute
  aws logs create-export-task --log-group-name $LAMBDA$i --from ${startTime} --to ${endTime} --destination "tianium.default" --destination-prefix $FILE$PREFIX$i
  sleep 2s
done
