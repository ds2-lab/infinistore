package proxy

import (
	"time"

	"github.com/wangaoone/LambdaObjectstore/src/proxy/lambdastore"
)

const LambdaMaxDeployments = 300
const NumLambdaClusters = 300
const LambdaStoreName = "LambdaStore"
const LambdaPrefix = "Store1VPCNode"
const InstanceWarmTimout = 1 * time.Minute

func init() {
	lambdastore.WarmTimout = InstanceWarmTimout
}
