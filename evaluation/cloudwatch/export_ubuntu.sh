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
TO=399
if [ "$4" != "" ] ; then
  FROM=$4
  TO=$4
fi
if [ "$5" != "" ] ; then
  TO=$5
fi

function wait_task(){
  RUNNING=$1
  # Query running task
  if [ "$RUNNING" == "" ] ; then
    RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
    if [ "$RUNNING" == "" ] ; then
      return 0
    fi
  fi
  
  # Wait for the end the last task
  for j in {0..15}
  do
    sleep 2s
    STATUSCODE=`aws logs describe-export-tasks --task-id "$RUNNING" | grep code | awk -F \" '{ print $4 }'`
    if [ "$STATUSCODE" != "COMPLETED" -a "$STATUSCODE" != "CANCELLED" -a "$STATUSCODE" != "FAILED" ] ; then
      continue
    elif [ "$STATUSCODE" == "COMPLETED" ] ; then
      echo "$RUNNING $STATUSCODE"
      return 0
    else
      echo "$RUNNING $STATUSCODE"
      return 1
    fi
  done

  # Abandon
  if [ "$STATUSCODE" == "RUNNING" ] ; then
    echo "Detect running task and wait timeout, killing task \"$RUNNING\"..."
    aws logs cancel-export-task --task-id \"$RUNNING\"
  fi

  wait_task $RUNNING
  return $?
}

# wait for tasks running now
wait_task

for (( i=$FROM; i<=$TO; i++ ))
do
  # try 3 times
  BACKOFF=2
  for k in {0..2}
  do
    echo "exporting $LAMBDA$LOG_PREFIX$i"
    RUNNING=`aws logs create-export-task --log-group-name $LAMBDA$LOG_PREFIX$i --from ${startTime} --to ${endTime} --destination "tianium.default" --destination-prefix $FILE$PREFIX$LOG_PREFIX$i | grep taskId | awk -F \" '{ print $4 }'`
    if [ "$RUNNING" == "" ] ; then
      if [ k == 2 ] ; then
        echo "abort"
      else
        echo "retry after ${BACKOFF}s"
      fi
      sleep ${BACKOFF}s
      ((BACKOFF=BACKOFF*2))
      continue
    else
      echo $RUNNING 
    fi

    # Wait or cancel task on timeout
    wait_task ${RUNNING}
    if [ $? == 0 ] ; then
      # pass
      break
    elif [ k == 2 ] ; then
      echo "abort"
    else
      echo "retry"
    fi
  done
done
