#!/bin/bash
LAMBDA="/aws/lambda/"
FILE="log/"
LOG_PREFIX="Store1VPCNode"

PREFIX=$1
start=$2
end=$3

# Convert date into seconds (Format is %s)
startTime=$(date -d "$start" +%s)000
endTime=$(date -d "$end" +%s)000

FROM=0
TO=999
if [ "$4" != "" ] ; then
  FROM=$4
  TO=$4
fi
if [ "$5" != "" ] ; then
  TO=$5
fi

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
  aws logs cancel-export-task --task-id \"$RUNNING\"

fi

# Wait another 30 seconds for the abandon procedure
for j in {0..15}
do
  RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
  if [ "$RUNNING" != "" ]; then
    sleep 2s
  else
    break
    echo "Done"
  fi
done

for (( i=$FROM; i<=$TO; i++ ))
do
  # try 3 times
  for k in {0..2}
  do
    echo "exporting $LAMBDA$LOG_PREFIX$i"
    aws logs create-export-task --log-group-name $LAMBDA$LOG_PREFIX$i --from ${startTime} --to ${endTime} --destination "tianium.default" --destination-prefix $FILE$PREFIX$LOG_PREFIX$i
    if [ $? != 0 ] ; then
      if [ k == 2 ] ; then
        echo "abort"
      else
        echo "retry"
      fi
      sleep 2s
      continue
    fi
    sleep 2s

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
      aws logs cancel-export-task --task-id \"$RUNNING\"
      if [ $? != 0 ] ; then
        echo "Done"
        sleep 2s
        break
      fi
    else
      echo "Done"
      sleep 2s
      break
    fi

    # Wait another 30 seconds for the abandon procedure
    for j in {0..15}
    do
      RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
      if [ "$RUNNING" != "" ]; then
        sleep 2s
      else
        break
      fi
    done

    echo "retry"
    sleep 2s
  done
done
