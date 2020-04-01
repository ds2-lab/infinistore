package server

import (
	"time"

	"github.com/wangaoone/LambdaObjectstore/proxy/lambdastore"
)

const LambdaMaxDeployments = 400
const NumLambdaClusters = 5

const (
	STEP               = 5               // number of step
	INTERVAL           = 25 * time.Second // minute
	InstanceWarmTimout = 1 * time.Minute
)

const LambdaStoreName = "LambdaStore"
const LambdaPrefix = "Proxy2Node"
const InstanceCapacity = 1536 * 1000000 // MB
const InstanceOverhead = 100 * 1000000  // MB
const Threshold = 0.8

var ActiveInstance int

func init() {
	lambdastore.WarmTimout = InstanceWarmTimout
}
