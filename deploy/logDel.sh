# deleting log in cloudWatch with prefix
#!/bin/bash

PREFIX="Store1VPCNode"

for ((i = 0; i <= $1; i++)); do
    aws logs delete-log-group --log-group-name /aws/lambda/$PREFIX$i
done
