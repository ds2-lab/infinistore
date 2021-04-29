# Evaluation

## deployment

AMI: ubuntu-xenial-16.04
Golang: 1.12

~~~
sudo add-apt-repository ppa:longsleep/golang-backports
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get install golang-go

echo "export GOPATH=$HOME/go" >> $HOME/.bashrc
. $HOME/.bashrc
mkdir -p $HOME/go/src/github.com/wangaoone
cd $HOME/go/src/github.com/wangaoone
git clone https://github.com/wangaoone/LambdaObjectstore.git LambdaObjectstore
git clone https://github.com/mason-leap-lab/redeo.git redeo
git clone https://github.com/wangaoone/redbench.git redbench

cd LambdaObjectstore/
git checkout config/[tianium]
git pull
cd src
go get

cd $HOME/go/src/github.com/wangaoone/redbench
go get

sudo apt install awscli
cd $HOME/go/src/github.com/wangaoone/LambdaObjectstore/evaluation
make deploy
~~~

## Data processing

### Collecting

For specified experiment prefix in date format: e.g. 202011070320

~~~
export EXPERIMENT=202011070320
~~~

To collect logs on the proxy:

~~~
./download ${EXPERIMENT}
~~~

To collect data collected in lambda nodes from S3, such as lambda side request logs, and recovery performance data.

~~~
cloudwatch/download.sh ${EXPERIMENT} data
~~~

To collect exported cloudwatch logs from S3

~~~
cloudwatch/download.sh ${EXPERIMENT} log
~~~

### Preprocessing


### Workload Processing Log

~~~
# Unzip
mkdir -p downloaded/proxy/${EXPERIMENT}
tar -xzf downloaded/proxy/${EXPERIMENT}.tar.gz -C downloaded/proxy/${EXPERIMENT}/

# Decode .clog file
./infla.sh ${EXPERIMENT}

# Extract cluster data from proxy output
cat downloaded/${EXPERIMENT}/simulate-400_proxy.csv | grep cluster, > downloaded/${EXPERIMENT}/cluster.csv
cat downloaded/${EXPERIMENT}/simulate-400_proxy.csv | grep bucket, > downloaded/${EXPERIMENT}/bucket.csv

# Extract billing info from cloudwatch log
cloudwatch/parse.sh downloaded/log/${EXPERIMENT}
cat downloaded/log/${EXPERIMENT}_bill.csv | grep invocation > downloaded/${EXPERIMENT}/bill.csv
make build-data
bin/preprocess -o downloaded/${EXPERIMENT}/recovery.csv -processor workload downloaded/data/${EXPERIMENT}
~~~