package invoker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type LocalInvoker struct {
}

func (ivk *LocalInvoker) InvokeWithContext(ctx context.Context, invokeInput *lambda.InvokeInput, opts ...request.Option) (*lambda.InvokeOutput, error) {
	var input protocol.InputEvent
	json.Unmarshal(invokeInput.Payload, &input)

	log.Println("invoking lambda...")

	args := make([]string, 0, 10)
	args = append(args, "-dryrun")
	args = append(args, fmt.Sprintf("-sid=%s", input.Sid))
	args = append(args, fmt.Sprintf("-cmd=%s", input.Cmd))
	args = append(args, fmt.Sprintf("-id=%d", input.Id))
	args = append(args, fmt.Sprintf("-proxy=%s", input.Proxy))
	args = append(args, fmt.Sprintf("-log=%d", input.Log))
	args = append(args, fmt.Sprintf("-flags=%d", input.Flags))
	if len(input.Status) > 0 {
		args = append(args, fmt.Sprintf("-term=%d", input.Status[0].Term))
		args = append(args, fmt.Sprintf("-updates=%d", input.Status[0].Updates))
		args = append(args, fmt.Sprintf("-diffrank=%f", input.Status[0].DiffRank))
		args = append(args, fmt.Sprintf("-hash=%s", input.Status[0].Hash))
		args = append(args, fmt.Sprintf("-snapshot=%d", input.Status[0].SnapshotTerm))
		args = append(args, fmt.Sprintf("-snapshotupdates=%d", input.Status[0].SnapshotUpdates))
		args = append(args, fmt.Sprintf("-snapshotsize=%d", input.Status[0].SnapshotSize))
		args = append(args, fmt.Sprintf("-tip=%s", input.Status[0].Tip))
	}

	cmd := exec.CommandContext(ctx, "lambda", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Println(err)
		return nil, err
	}

	statuscode := int64(200)
	output := &lambda.InvokeOutput{
		StatusCode: &statuscode,
	}
	return output, nil
}
