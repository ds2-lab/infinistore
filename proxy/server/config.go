package server

import (
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

const LambdaMaxDeployments = 400
const NumLambdaClusters = 12

const InstanceWarmTimout = 1 * time.Minute

const LambdaStoreName = "LambdaStore"
const LambdaPrefix = "Proxy2Node"
const InstanceCapacity = 1536 * 1000000 // MB
const InstanceOverhead = 100 * 1000000  // MB
const Threshold = 0.8 // scaling out avg instance size threshold

const AWSRegion = "us-east-1"

const ServerPublicIp = ""                  // Leave it empty if using VPC.
const RecoverRate = 40 * 1000000           // 40MB for 1536MB instance, 70MB for 3008MB instance.
const BackupsPerInstance = 36              // (InstanceCapacity - InstanceOverhead) / RecoverRate

func init() {
	lambdastore.WarmTimout = InstanceWarmTimout
}
