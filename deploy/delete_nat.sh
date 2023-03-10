#!/bin/bash

# Replace NAT_NAME with the name of the nat gateway. The name should be the same as the one used to create the nat gateway.
NAT_NAME="nat-lambda"

NATID=`aws ec2 describe-nat-gateways --filter "Name=tag:Name,Values=$NAT_NAME" --filter 'Name=state,Values=available' | grep NatGatewayId | awk -F \" '{ print $4 }'`

if [ "$NATID" == "" ]; then
  echo "No qualified nat gateway found."
  exit
fi

echo "Deleting $NATID"
aws ec2 delete-nat-gateway --nat-gateway-id $NATID
