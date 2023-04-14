# InfiniStore

**InfiniStore** is an elastic, cost-effective, and high-performance object storage built atop ephemeral cloud funtions. Built on top of the [InfiniCache](https://ds2-lab.github.io/infinicache/) codebase, InfiniStore offers automatic elasticity, durability, strong consistency, and high performance.

The preprint of our VLDB'23 paper can be viewed at: [InfiniStore: Elastic Serverless Cloud Storage](https://arxiv.org/abs/2209.01496).

## Prepare

- ### EC2 Proxy

  Amazon EC2 AMI: ubuntu-18.04

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

  Go to AWS IAM console and create a role for the Lambda memory node (Lambda function).

  AWS IAM console -> Roles -> Create Role -> Lambda ->

  **`AWSLambdaFullAccess, `**

  **`AWSLambdaVPCAccessExecutionRole, `**

  **`AWSLambdaENIManagementAccess`**

  #### Enable Lambda Internet Access under VPC

  

- ### S3

  Create the S3 bucket to store the zip file of the Lambda code and data output from Lambda functions. Remember the name of this bucket for the configuration in next step.

- ### Configuration

  #### Lambda Function Configuration and Creation

  Edit the aws settings and the VPC configuration in `deploy/config.yml`. If you do not want to run InfiniStore under VPC, skip `aws-vpc-subnets` and `aws-security-groups` settings. A valid configuration will like:
  ```yml
  region: "us-east-1"
  aws-iam-role: "arn:aws:iam::1234567890:role/jzhang33"
  aws-vpc-subnets: "subnet-12345abcdef,subnet-67890abcdef"
  aws-security-groups: "sg-abcdef0987654321"
  s3-bucket-cos: "infinistore.cos"    # S3 bucket for InfiniStore COS layer.
  s3-bucket-data: "infinistore.data"  # Optional. S3 bucket for collecting data required for reproducibility experiments.
  ```

  Edit `deploy/create_function.sh` and `deploy/update_function.sh`
  ```shell
  DEPLOY_PREFIX="your lambda function prefix"
  DEPLOY_CLUSTER=1000   # The number of Lambda deployments used for window rotation.
  DEPLOY_MEM=1536       # The memory of Lambda deployments.
  S3="your bucket name" # S3 bucket for uploading the binary of AWS Lambda functions.
  ```

  Run script to create and deploy lambda functions (Also, if you do not want to run InfiniStore under VPC, you need to remove the `--no-vpc` flag on executing `deploy/create_function.sh`).

  ```shell
  go get
  deploy/create_function.sh --no-vpc 600
  ```

  If lambda functions are deployed in VPC, create NAT gateway to give Lambdas access to the proxy. You may use AWS console or `create_nat.sh`. Besure to change the settings as described in the script before executing. Plese [refer to this article](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/).

  ```shell
  deploy/create_nat.sh
  ```

  #### Proxy Configuration

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

Benchmark tool and workload replayer [infinibench](https://github.com/ds2-lab/infinibench).

RESP (REdis Serialization Protocol) library [redeo](https://github.com/mason-leap-lab/redeo).   



## To cite InfiniStore

```
@inproceedings {vldb23-infinistore,
  author       = {Jingyuan Zhang and Ao Wang and Xiaolong Ma and Benjamin Carver and Nicholas John Newman and Ali Anwar and Lukas Rupprecht and Dimitrios Skourtis and Vasily Tarasov and Feng Yan and Yue Cheng},
  title        = {InfiniStore: Elastic Serverless Cloud Storage},
  journal      = {Proc. {VLDB} Endow.},
  volume       = {16},
  number       = {7},
  pages        = {1629--1642},
  year         = {2023},
  url          = {https://www.vldb.org/pvldb/vol16/p1629-zhang.pdf},
}

```


## Contributing

Please feel free to hack on InfiniStore! We're happy to accept contributions.

