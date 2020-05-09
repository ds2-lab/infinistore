package config

import (
	"time"
)

// AWSRegion Region of AWS services.
const AWSRegion = "us-east-1"

// LambdaMaxDeployments Number of Lambda function deployments available.
const LambdaMaxDeployments = 400

// NumLambdaClusters Number of Lambda function deployments initiated on launching.
const NumLambdaClusters = 100

// LambdaStoreName Obsoleted. Name of Lambda function for replica version.
const LambdaStoreName = "LambdaStore"

// LambdaPrefix Prefix of Lambda function.
const LambdaPrefix = "Store1VPCNode"

// InstanceWarmTimout Interval to warmup Lambda functions.
const InstanceWarmTimout = 1 * time.Minute

// InstanceCapacity Capacity of deployed Lambda functions.
const InstanceCapacity = 1536 * 1000000    // MB

// InstanceOverhead Memory reserved for running program on Lambda functions.
const InstanceOverhead = 100 * 1000000     // MB

// ServerPublicIp Public IP of proxy, leave empty if running Lambda functions in VPC.
const ServerPublicIp = ""                  // Leave it empty if using VPC.

// RecoverRate Empirical S3 download rate for specified InstanceCapacity.
const RecoverRate = 40 * 1000000           // 40MB for 1536MB instance, 70MB for 3008MB instance.

// BackupsPerInstance  Number of backup instances used for parallel recovery.
const BackupsPerInstance = 36              // (InstanceCapacity - InstanceOverhead) / RecoverRate

// ProxyList Ip addresses of proxies.
var ProxyList []string
