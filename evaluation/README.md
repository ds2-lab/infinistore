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
tar -xzf downloaded/data/202011070320.tar.gz

# Decode .clog file
./infla.sh 202011070320

# Extract cluster data from proxy output
cat downloaded/202011070320/simulate-400_proxy.csv | grep cluster, > downloaded/202011070320/cluster.csv
~~~
