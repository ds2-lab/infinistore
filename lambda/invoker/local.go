package invoker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/hidez8891/shm"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

const (
	ShmSize = 4096
)

var (
	ErrNodeMismatched = errors.New("node mismatched")
)

type LocalOutputPayloadSetter func(string, []byte)

// LocalInvoker Invoke local lambda function simulation
// Use throttle to simulate Lambda network: https://github.com/sitespeedio/throttle
// throttle --up 800000 --down 800000 --rtt 1 (800MB/s, 1ms)
// throttle stop
// Use container to simulate Lambda resouce limit
type LocalInvoker struct {
	SetOutputPayload LocalOutputPayloadSetter
	nodeName         string
	nodeCached       *shm.Memory
	invokeId         int32
	mu               sync.Mutex
	closed           chan struct{}
	rsp              chan []byte
}

func (ivk *LocalInvoker) InvokeWithContext(ctx context.Context, invokeInput *lambda.InvokeInput, opts ...request.Option) (*lambda.InvokeOutput, error) {
	// Do some initialization for first invocation.
	if ivk.rsp == nil {
		ivk.rsp = make(chan []byte, 1)
		ivk.SetOutputPayload = func(sid string, payload []byte) {
			ivk.rsp <- payload
		}
	}

	log.Printf("invoking lambda %s...\n", *invokeInput.FunctionName)
	cached := ivk.nodeCached
	if cached == nil {
		ivk.mu.Lock()
		if ivk.nodeCached == nil {
			// Create shared memory for function reinvoking, so the local lambda can be reused.
			if cache, err := shm.Create(*invokeInput.FunctionName, ShmSize); err != nil {
				ivk.mu.Unlock()
				return nil, err
			} else {
				ivk.nodeCached = cache
				ivk.nodeName = *invokeInput.FunctionName
				ivk.closed = make(chan struct{})
			}

			if err := ivk.execLocked(ctx, invokeInput); err != nil {
				ivk.mu.Unlock()
				return nil, err
			}
		} else {
			cached = ivk.nodeCached
		}
		ivk.mu.Unlock()
	}
	if cached != nil {
		if ivk.nodeName != *invokeInput.FunctionName {
			ivk.close()
			return nil, ErrNodeMismatched
		}

		// Reinvoke the local lambda function.
		cached.Seek(0, io.SeekStart)
		buffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(buffer, uint32(len(invokeInput.Payload)))
		if _, err := cached.Write(buffer); err != nil {
			ivk.close()
			return nil, err
		}
		if _, err := cached.Write(invokeInput.Payload); err != nil {
			ivk.close()
			return nil, err
		}
	}

	statuscode := int64(200)
	output := &lambda.InvokeOutput{
		StatusCode: &statuscode,
		Payload:    <-ivk.rsp, // Wait for response.
	}
	return output, nil
}

func (ivk *LocalInvoker) Close() {
	cached := ivk.nodeCached
	if cached != nil {
		cached.Seek(0, io.SeekStart)

		bye := protocol.InputEvent{Cmd: protocol.CMD_BYE}
		paylood, _ := json.Marshal(&bye)

		buffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(buffer, uint32(len(paylood)))
		if _, err := ivk.nodeCached.Write(buffer); err != nil {
			ivk.close()
			return
		}
		if _, err := cached.Write(paylood); err != nil {
			ivk.close()
			return
		}
		// Wait for confirming closing.
		<-ivk.closed
	}
}

func (ivk *LocalInvoker) close() {
	ivk.mu.Lock()
	defer ivk.mu.Unlock()

	ivk.closeLocked()
}

func (ivk *LocalInvoker) execLocked(ctx context.Context, invokeInput *lambda.InvokeInput) error {
	var input protocol.InputEvent
	if err := json.Unmarshal(invokeInput.Payload, &input); err != nil {
		return err
	}

	args := make([]string, 0, 10)
	args = append(args, "-dryrun")
	args = append(args, fmt.Sprintf("-name=%s", *invokeInput.FunctionName))
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

	ivk.invokeId++
	invokeId := ivk.invokeId
	log.Printf("Start lambda %s...\n", *invokeInput.FunctionName)
	cmd := exec.CommandContext(ctx, "bin/lambda", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return err
	}

	go func() {
		if err := cmd.Wait(); err != nil {
			log.Printf("lambda %s exited with error: %v\n", *invokeInput.FunctionName, err)
		}
		ivk.mu.Lock()
		defer ivk.mu.Unlock()

		// Avoid closing a new invocation with different invokeId
		if ivk.invokeId == invokeId {
			ivk.closeLocked()
			close(ivk.closed)
		}
	}()

	return nil
}

func (ivk *LocalInvoker) closeLocked() {
	if ivk.nodeCached != nil {
		ivk.nodeCached.Close()
		ivk.nodeCached = nil
	}
}
