package invoker

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
)

type FunctionInvoker interface {
	InvokeWithContext(context.Context, *lambda.InvokeInput, ...request.Option) (*lambda.InvokeOutput, error)
}
