package config

import (
	"time"
)

// AWSRegion Region of AWS services.
const AWSRegion = "us-east-1"

// LambdaMaxDeployments Number of Lambda function deployments available.
const LambdaMaxDeployments = 400

// Mode of cluster.
const StaticCluster = "static"
const WindowCluster = "window"
const Cluster = StaticCluster

// NumLambdaClusters Number of Lambda function deployments initiated on launching.
// For window cluster, this must be at least D+P
const NumLambdaClusters = 400

// LambdaStoreName Obsoleted. Name of Lambda function for replica version.
const LambdaStoreName = "LambdaStore"

// LambdaPrefix Prefix of Lambda function.
const LambdaPrefix = "Store1VPCNode"

// InstanceWarmTimout Interval to warmup Lambda functions.
const InstanceWarmTimout = 1 * time.Minute

// Instance degrade warmup interval
const InstanceDegradeWarmTimout = 5 * time.Minute

// InstanceCapacity Capacity of deployed Lambda functions.
// TODO: Detectable on invocation. Can be specified by option -funcap for now.
const DefaultInstanceCapacity = 1536 * 1000000 // MB

// InstanceOverhead Memory reserved for running program on Lambda functions.
const InstanceOverhead = 300 * 1000000 // MB

// Threshold Scaling out avg instance size threshold
const Threshold = 0.8 // Don't set beyond 0.8

// Maximum chunk per instance
const ChunkThreshold = 125000 // Fraction, ChunkThreshold = InstanceCapacity / 100K * Threshold

// ServerPublicIp Public IP of proxy, leave empty if running Lambda functions in VPC.
const ServerPublicIp = "" // Leave it empty if using VPC.

// RecoverRate Empirical S3 download rate for specified InstanceCapacity.
// 40MB for 512, 1024, 1536MB instance, 70MB for 3008MB instance.
const RecoverRate = 40 * 1000000 // Not actually used.

// BackupsPerInstance  Number of backup instances used for parallel recovery.
const BackupsPerInstance = 20 // (InstanceCapacity - InstanceOverhead) / RecoverRate

// Each bucket's active duration
const BucketDuration = 10 // min

// Async migrate control
const ActiveReplica = 2 //min

// ProxyList Ip addresses of proxies.
var ProxyList []string
