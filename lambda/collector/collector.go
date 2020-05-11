package collector

import (
	"bytes"
	"fmt"
	"os/exec"
	"io"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"strings"
	"sync"

	"github.com/mason-leap-lab/infinicache/lambda/lifetime"
)

const (
	COLLECT_REQUEST = 0x0001
	COLLECT_PERSIST = 0x0002
)

var (
	AWSRegion      string
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
	// Wait for data depository.
	dataDeposited.Wait()

	data := new(bytes.Buffer)
	for _, entry := range dataDepository {
		entry.WriteTo(data)
	}

	key := fmt.Sprintf("%s/%s/%d", Prefix, FunctionName, Lifetime.Id())
	s3Put(S3Bucket, key, data)
	dataDepository = dataDepository[:0]
}

func s3Put(bucket string, key string, body io.Reader) {
	// The session the S3 Uploader will use
	sess := awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
		SharedConfigState: awsSession.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String(AWSRegion)},
	}))

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

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

	log.Info("Data uploaded to S3: %v", result.Location)
}
