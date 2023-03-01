# Evaluation

This document describes how to use scripts to reproduce InfiniStore experiments.

## Deployment

AMI: ubuntu-18.04
Golang: 1.18

~~~bash
sudo add-apt-repository ppa:longsleep/golang-backports
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get -y install golang-go awscli zip

mkdir $HOME/go
echo "export GOPATH=$HOME/go" >> $HOME/.bashrc
echo "export PATH=\$PATH:\$GOPATH/bin" >> $HOME/.bashrc
. $HOME/.bashrc
go install github.com/ScottMansfield/nanolog/cmd/inflate@v0.2.0
git clone https://github.com/ds2-lab/infinistore.git infinistore
git clone https://github.com/ds2-lab/infinibench.git infinibench
git clone https://github.com/ds2-lab/infinistore-reproducibility.git infinistore-reproducibility

cd infinistore/evaluation
# git checkout config/[tianium] # optionally checkout the configuration branch
git pull

# Edit config.mk as described below.

make deploy
~~~

### Configuration

Following the comments in `config.mk`, configure the experiment settings. Two essential entries may need to change (Note that no space around "="):
```shell
REGION=us-east-1 # AWS region
S3_BUCKET_DATA=ds2-lab.datapool # S3 bucket for storing the experiment data.
```

## Execution

Execute targets in `Makefile`:
```bash
make [target]
```

## Data processing

### Collecting

For specified experiment prefix in date format: e.g. 202011070320.

~~~bash
export EXPERIMENT=202011070320
~~~

To collect logs on the proxy, config `./download` as commented and execute (add "-" to process data locally):

~~~bash
./download ${EXPERIMENT} [-]
~~~

To collect data collected in Lambda nodes from S3, such as Lambda side request logs, and recovery performance data.

~~~bash
cloudwatch/download.sh ${EXPERIMENT} data
~~~

To collect exported cloudwatch logs from S3

~~~bash
cloudwatch/download.sh ${EXPERIMENT} log
~~~

### Preprocessing

~~~bash
workload/preprocess.sh ${EXPERIMENT}
~~~

The above script is an aggregation of following commands, execute them indivisually if something wrong.

~~~bash
# Unzip data from the proxy.
mkdir -p downloaded/proxy/${EXPERIMENT}
tar -xzf downloaded/proxy/${EXPERIMENT}.tar.gz -C downloaded/proxy/${EXPERIMENT}/

# Decode .clog file. Please ensure command "inflate" is installed during deployment.
./infla.sh ${EXPERIMENT}

# Extract cluster data from proxy output
cat downloaded/${EXPERIMENT}/simulate-400_proxy.csv | grep cluster, > downloaded/${EXPERIMENT}/cluster.csv
cat downloaded/${EXPERIMENT}/simulate-400_proxy.csv | grep bucket, > downloaded/${EXPERIMENT}/bucket.csv

# Extract billing info from cloudwatch log
cloudwatch/parse.sh downloaded/log/${EXPERIMENT}
cat downloaded/log/${EXPERIMENT}_bill.csv | grep invocation > downloaded/${EXPERIMENT}/bill.csv
make build-data
# Replace "[FunctionPrefix]" with the prefix of Lambda functions as "DEPLOY_PREFIX" or "--prefix" on executing deploy/create_function.sh.
bin/preprocess -o downloaded/${EXPERIMENT}/recovery.csv -processor workload -fprefix [FunctionPrefix] downloaded/data/${EXPERIMENT}
~~~