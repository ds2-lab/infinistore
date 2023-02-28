#!/bin/bash

# Replace SUBNET_ID with the subnet id of proxies
SUBNET_ID="subnet-0f276387ac4cba169"
# Replace ALLOCATION_ID with the id of the elastic ip
ALLOCATION_ID="eipalloc-02d452e00ee147b21"
# Replace ROUTE_TABLE_ID with the id of the route table corresponding to the subnet
ROUTE_TABLE_ID="rtb-00c1d9dfbfbc3ef90"
# Replace NAT_NAME with the name of the nat gateway. The name is used to identify the nat gateway on deleting.
NAT_NAME="nat-lambda"

NATID=`aws ec2 describe-nat-gateways --filter "Name=tag:Name,Values=$NAT_NAME" --filter 'Name=state,Values=available' | grep NatGatewayId | awk -F \" '{ print $4 }'`

if [ "$NATID" == "" ]; then

  aws ec2 create-nat-gateway --subnet-id $SUBNET_ID --allocation-id $ALLOCATION_ID --tag-specifications "ResourceType=natgateway,Tags=[{Key=Name,Value=$NAT_NAME}]"

  for j in {0..2}
  do
    NATID=`aws ec2 describe-nat-gateways --filter "Name=tag:Name,Values=$NAT_NAME" --filter 'Name=state,Values=available' | grep NatGatewayId | awk -F \" '{ print $4 }'`
    if [ "$NATID" == "" ]; then
      sleep 2s
    else
      break
    fi
  done

  # Abandon
  if [ "$NATID" == "" ]; then
    echo "Wait for nat gateway timeout. Failed to create the nat gateway"
    exit 1
  fi
fi

aws ec2 create-route --route-table-id $ROUTE_TABLE_ID --destination-cidr-block 0.0.0.0/0 --nat-gateway-id $NATID
