package collector

import (
	"bytes"
	"fmt"
	"os/exec"
	"io"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mason-leap-lab/infinicache/common/logger"
	// "runtime"
	// "runtime/pprof"
	"strings"
	"sync"

	"github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	COLLECT_REQUEST = 0x0001
	COLLECT_PERSIST = 0x0002
)

var (
	S3Bucket       string

	Prefix         string
	HostName       string
	FunctionName   string
	Enables        int32 = COLLECT_PERSIST
	Lifetime       *lifetime.Lifetime
	Session        *lifetime.Session

	dataGatherer                   = make(chan DataEntry, 10)
	dataDepository                 = make([]DataEntry, 0, 100)
	dataDeposited  sync.WaitGroup
	log            logger.ILogger  = &logger.ColorLogger{ Prefix: "collector ", Level: logger.LOG_LEVEL_INFO }
)

func init() {
	cmd := exec.Command("uname", "-a")
	host, err := cmd.CombinedOutput()
	if err != nil {
		log.Debug("cmd.Run() failed with %s\n", err)
	}

	HostName = strings.Split(string(host), " #")[0]
	log.Debug("hostname is: %s", HostName)

	FunctionName = lambdacontext.FunctionName
}

type DataEntry interface {
	WriteTo(*bytes.Buffer)
}

func Send(entry DataEntry) {
	dataDeposited.Add(1)
	dataGatherer <- entry
}

func Collect(session *lifetime.Session) {
	session.CleanUp.Add(1)
	defer session.CleanUp.Done()

	for {
		select {
		case <-session.WaitDone():
			return
		case entry := <-dataGatherer:
			dataDepository = append(dataDepository, entry)
			dataDeposited.Done()
		}
	}
}

func Save() {
	SaveWithOption(false)
}

func SaveWithOption(snapshot bool) {
	// Wait for data depository.
	dataDeposited.Wait()
	cnt := len(dataDepository)

	data := new(bytes.Buffer)
	for _, entry := range dataDepository {
		entry.WriteTo(data)
		data.WriteString("\n")
	}

	// memprof := new(bytes.Buffer)
	// runtime.GC()
	// pprof.WriteHeapProfile(memprof)

	key := fmt.Sprintf("%s/%s/%d", Prefix, FunctionName, Lifetime.Id())
	s3Put(S3Bucket, key, data, cnt)
	// memkey := fmt.Sprintf("%s/%s/%d.mem.prof", Prefix, FunctionName, Lifetime.Id())
	// s3Put(S3Bucket, memkey, memprof)
	if !snapshot {
		dataDepository = dataDepository[:0]
	}
}

func s3Put(bucket string, key string, body io.Reader, num int) {
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(types.AWSSession())

	upParams := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   body,
	}
	// Perform an upload.
	result, err := uploader.Upload(upParams)
	if err != nil {
		log.Error("Failed to upload data: %v", err)
		return
	}

	log.Info("Data(%d) uploaded to S3: %v", num, result.Location)
}
