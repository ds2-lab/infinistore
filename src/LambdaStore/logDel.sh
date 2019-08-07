# deleting log in cloudWatch with prefix
#!/bin/bash

PREFIX=$1

for i in {0..15}
do
    aws logs delete-log-group --log-group-name /aws/lambda/$PREFIX$i
done
