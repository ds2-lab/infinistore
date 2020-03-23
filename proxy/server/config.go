package server

import (
	"time"

	"github.com/wangaoone/LambdaObjectstore/proxy/lambdastore"
)

const LambdaMaxDeployments = 400
const NumLambdaClusters = 200

const LambdaStoreName = "LambdaStore"
const LambdaPrefix = "Proxy2Node"
const InstanceWarmTimout = 1 * time.Minute
const InstanceCapacity = 1536 * 1000000 // MB
const InstanceOverhead = 100 * 1000000  // MB
const Threshold = 0.8

func init() {
	lambdastore.WarmTimout = InstanceWarmTimout
}
