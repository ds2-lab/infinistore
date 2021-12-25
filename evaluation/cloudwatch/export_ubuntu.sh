#!/bin/bash
LAMBDA="/aws/lambda/"
FILE="log/"
LOG_PREFIX="Store1VPCNode"
COLOROK="\e[32m"
COLORFAIL="\e[31m"
ENDCOLOR="\e[0m"

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
    printf "Waiting to finish, task-id $RUNNING "
  fi
  
  # Wait for the end the last task: timeout in 10 min
  for j in {0..300}
  do
    sleep 2s
    STATUSCODE=`aws logs describe-export-tasks --task-id "$RUNNING" | grep code | awk -F \" '{ print $4 }'`
    if [ "$STATUSCODE" != "COMPLETED" -a "$STATUSCODE" != "CANCELLED" -a "$STATUSCODE" != "FAILED" ] ; then
      printf "."
      continue
    elif [ "$STATUSCODE" == "COMPLETED" ] ; then
      echo -e " ${COLOROK}${STATUSCODE}${ENDCOLOR}"
      return 0
    else
      echo -e " ${COLORFAIL}${STATUSCODE}${ENDCOLOR}"
      return 1
    fi
  done

  # Abandon
  if [ "$STATUSCODE" == "RUNNING" ] ; then
    printf " Timeout, cancelling..."
    aws logs cancel-export-task --task-id $RUNNING
  fi

  wait_task $RUNNING
  return $?
}

# wait for tasks running now
wait_task
echo "" #Blank to separate tasks

for (( i=$FROM; i<=$TO; i++ ))
do
  # try 3 times
  BACKOFF=2
  for k in {0..10}
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
      printf "task-id $RUNNING "
    fi

    # Wait or cancel task on timeout
    wait_task ${RUNNING}
    if [ $? == 0 ] ; then
      # pass
      break
    elif [ k == 2 ] ; then
      echo -e "${COLORFAIL}Abort${ENDCOLOR}"
    else
      echo -e "${COLOROK}Retry${ENDCOLOR}"
    fi
  done

  echo "" #Blank to separate tasks
done
