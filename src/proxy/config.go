package proxy

import (
	"time"

	"github.com/wangaoone/LambdaObjectstore/src/proxy/lambdastore"
)

const LambdaMaxDeployments = 400
const NumLambdaClusters = 400
const LambdaStoreName = "LambdaStore"
const LambdaPrefix = "Store1VPCNode"
const InstanceWarmTimout = 1 * time.Minute

func init() {
	lambdastore.WarmTimout = InstanceWarmTimout
}
