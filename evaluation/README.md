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

To collect logs on the proxy:

~~~
./download 202011070320
~~~

To collect data collected in lambda nodes from S3, such as lambda side request logs, and recovery performance data.

~~~
cloudwatch/download.sh 202011070320 data
~~~

To collect exported cloudwatch logs from S3

~~~
cloudwatch/download.sh 202011070320 log
~~~

### Preprocessing


### Workload Processing Log

~~~
# Unzip
mkdir -p downloaded/proxy/202011070320
tar -xzf downloaded/proxy/202011070320.tar.gz -C downloaded/proxy/202011070320/

# Decode .clog file
./infla.sh 202011070320

# Extract cluster data from proxy output
cat downloaded/202011070320/simulate-400_proxy.csv | grep cluster, > downloaded/202011070320/cluster.csv
cat downloaded/202011070320/simulate-400_proxy.csv | grep bucket, > downloaded/202011070320/bucket.csv

# Extract billing info from cloudwatch log
cloudwatch/parse.sh downloaded/log/202011070320
cat downloaded/log/202011070320_bill.csv | grep invocation > downloaded/202011070320/bill.csv
make build-data
bin/preprocess -o downloaded/202011070320/recovery.csv -processor workload downloaded/data/202011070320
~~~