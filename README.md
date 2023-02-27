# InfiniStore

**InfiniStore** is an elastic, cost-effectiveness and high-performance object storage that is built atop ephemeral cloud funtions. Built based on [InfiniCache](https://ds2-lab.github.io/infinicache/) codebase, InfiniStore offers automatic elasticity, durability, strong consistency.

The preprint of VLDB'23 paper is accessible now as: [Sion: Elastic Serverless Cloud Storage](https://arxiv.org/abs/2209.01496)

## Prepare

- ### EC2 Proxy

  Amazon EC2 AMI: ubuntu-xenial-18.04

  Golang version: 1.18

  Be sure the port **6378 - 6379** is avaiable on the proxy

  We recommend that EC2 proxy and Lambda functions are under the same VPC network, and deploy InfiniStore proxy on EC2 instances with high bandwidth (`c5n` family maybe a good choice).

- ### Golang install

  Jump to [install_go.md](https://github.com/ds2-lab/infinistore/blob/master/install_go.md)

- ### Package install

  Install basic package
  ```shell
  sudo apt-get update
  sudo apt-get -y upgrade
  sudo apt install awscli
  sudo apt install zip
  ```

  Clone this repo
  ```shell
  git clone https://github.com/ds2-lab/infinistore.git
  ```

  Run `aws configure` to setup your AWS credential.
  ```shell
  aws configure
  ```

- ### Lambda Runtime

  #### Lambda Role setup

  Go to AWS IAM console and create a role for the lambda cache node (Lambda function).

  AWS IAM console -> Roles -> Create Role -> Lambda ->

  **`AWSLambdaFullAccess, `**

  **`AWSLambdaVPCAccessExecutionRole, `**

  **`AWSLambdaENIManagementAccess`**

  #### Enable Lambda internet access under VPC

  Plese [refer to this article](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/). (You could skip this step if you do not want to run InfiniStore under VPC).

- ### S3

  Create the S3 bucket to store the zip file of the Lambda code and data output from Lambda functions. Remember the name of this bucket for the configuration in next step.

- ### Configuration

  #### Lambda function create and config

  Edit `deploy/create_function.sh` and `deploy/update_function.sh`
  ```shell
  DEPLOY_PREFIX="your lambda function prefix"
  DEPLOY_CLUSTER=400 # The number of Lambda deployments used for window rotation.
  DEPLOY_MEM=1024 # The memory of Lambda deployments.
  S3="your bucket name"
  ```

  Edit destination S3 bucket in `lambda/config.go`, these buckets are for data collection and durable storage.
  ```go
  S3_BACKUP_BUCKET = "your COS bucket%s"  // Leave %s at the end your COS bucket.
  S3_COLLECTOR_BUCKET = "your data collection bucket" // Optional. Required for reproducibility experiments.
  ```

  Edit `lambda/migrator/client.go`,  change AWS region if necessary.
  ```go
  AWSRegion = "us-east-1"
  ```

  Edit the aws settings and the VPC configuration in `deploy/deploy_function.go`. If you do not want to run InfiniStore under VPC, you do not need to modify the `subnet` and `securityGroup` settings.

  ```go
  ROLE = "arn:aws:iam::[aws account id]:role/[role name]"
  REGION = "us-east-1"
  ...
  ...
  subnet = []*string{
    aws.String("your private subnet 1"),
    aws.String("your private subnet 2"),
  }
  securityGroup = []*string{
    aws.String("your security group")
  }
  ```

  Run script to create and deploy lambda functions (Also, if you do not want to run InfiniStore under VPC, you need to remove the `--no-vpc` flag on executing `deploy/create_function.sh`).

  ```shell
  export GO111MODULE="on"
  go get
  deploy/create_function.sh --no-vpc 600
  ```

  If lambda functions are deployed in VPC, create NAT gateway to give lambdas access to the proxy. You may use AWS console or `create_nat.sh`. Besure to change the settings as described in the script before executing.

  ```shell
  deploy/create_nat.sh
  ```

  #### Proxy configuration

  Edit `proxy/config/config.go`, change the aws region, cluster size, and prefix of the Lambda functions.
  ```go
  const AWSRegion = "us-east-1"
  const NumLambdaClusters = 1000
  const LambdaPrefix = "Your Lambda Function Prefix"
  const ServerPublicIp = ""  // Leave it empty if using VPC.
  ```

## Execution

- Proxy server

  Run `make start` for quick starting the proxy server. The proxy will show a console dashboard to track Lambda invocation. Logs by default is in file `log` in the same folder. `debug` option is available for more detail logs.

  ```bash
  make start ["PARAMS=--debug"]
  ```

  To stop proxy server, press `ctrl+c` or `q` if dashboard is displayed, otherwise, run `make stop`. If `make stop` were not working, you could use `pgrep proxy`, `pgrep go`, or check `/tmp/infinistore.pid` to find the pid and kill the proxy.

- Client library

  The toy demo for Client Library

  ```bash
  go run client/example/main.go
  ```

  The result should be

  ```bash
  ~$ go run client/example/main.go
  2023/02/26 23:07:48 EcRedis Set foo 15 160250633
  2023/02/26 23:07:48 EcRedis Got foo 15 28614169 ( 28574763 34758 )
  GET foo:Hello infinity!(28.698376ms)
  ```

- Stand-alone local simulation

  On Mac, InfiniStore can be run in local simulation mode, which will initiate local processes as cloud functions.

  Enable local function execution by editing `lambda/config.go`:
  ```go
  S3_BACKUP_BUCKET = "your S3 bucket%s"  // Leave %s at the end your S3 bucket.
  DRY_RUN = true
  ```

  Run `make start-local` to start a stand-alone local proxy server, which will invoke functions locally to simulation Lambda execution.

  Run `make test` to put/get a toy object.

## Related repos

Benchmark tool and workload replayer [redbench](https://github.com/wangaoone/redbench)

RESP (REdis Serialization Protocol) library [redeo](https://github.com/mason-leap-lab/redeo)  