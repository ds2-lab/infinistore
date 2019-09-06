# deleting log in cloudWatch with prefix
#!/bin/bash

PREFIX="Store1VPCNode"

for i in {0..299}
do
    aws logs delete-log-group --log-group-name /aws/lambda/$PREFIX$i
done
