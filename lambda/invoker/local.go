package invoker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type LocalOutputPayloadSetter func(string, []byte)

// LocalInvoker Invoke local lambda function simulation
// Use throttle to simulate Lambda network: https://github.com/sitespeedio/throttle
// throttle --up 800000 --down 800000 --rtt 1 (800MB/s, 1ms)
// throttle stop
// Use container to simulate Lambda resouce limit
type LocalInvoker struct {
	SetOutputPayload LocalOutputPayloadSetter
}

func (ivk *LocalInvoker) InvokeWithContext(ctx context.Context, invokeInput *lambda.InvokeInput, opts ...request.Option) (*lambda.InvokeOutput, error) {
	var input protocol.InputEvent
	json.Unmarshal(invokeInput.Payload, &input)

	log.Printf("invoking lambda %d...\n", input.Id)

	args := make([]string, 0, 10)
	args = append(args, "-dryrun")
	args = append(args, fmt.Sprintf("-sid=%s", input.Sid))
	args = append(args, fmt.Sprintf("-cmd=%s", input.Cmd))
	args = append(args, fmt.Sprintf("-id=%d", input.Id))
	args = append(args, fmt.Sprintf("-proxy=%s", input.Proxy))
	args = append(args, fmt.Sprintf("-log=%d", input.Log))
	args = append(args, fmt.Sprintf("-flags=%d", input.Flags))
	if len(input.Status.Metas) > 0 {
		args = append(args, fmt.Sprintf("-term=%d", input.Status.Metas[0].Term))
		args = append(args, fmt.Sprintf("-updates=%d", input.Status.Metas[0].Updates))
		args = append(args, fmt.Sprintf("-diffrank=%f", input.Status.Metas[0].DiffRank))
		args = append(args, fmt.Sprintf("-hash=%s", input.Status.Metas[0].Hash))
		args = append(args, fmt.Sprintf("-snapshot=%d", input.Status.Metas[0].SnapshotTerm))
		args = append(args, fmt.Sprintf("-snapshotupdates=%d", input.Status.Metas[0].SnapshotUpdates))
		args = append(args, fmt.Sprintf("-snapshotsize=%d", input.Status.Metas[0].SnapshotSize))
		args = append(args, fmt.Sprintf("-tip=%s", input.Status.Metas[0].Tip))
	}
	if len(input.Status.Metas) > 1 {
		strMetas, _ := json.Marshal(input.Status.Metas[1:])
		args = append(args, fmt.Sprintf("-metas=%s", string(strMetas)))
	}
	// log.Printf("args: %v\n", args)

	cmd := exec.CommandContext(ctx, "lambda", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	ret := make(chan []byte, 1)
	ivk.SetOutputPayload = func(sid string, payload []byte) {
		if input.Sid == sid {
			ret <- payload
		}
		close(ret)
	}

	var wg sync.WaitGroup
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = cmd.Run()
	}()
	wg.Wait()

	if err != nil {
		return nil, err
	}

	statuscode := int64(200)
	output := &lambda.InvokeOutput{
		StatusCode: &statuscode,
		Payload:    <-ret,
	}
	ivk.SetOutputPayload = nil
	return output, nil
}
