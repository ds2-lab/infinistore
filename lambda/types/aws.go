package types

import (
	"github.com/aws/aws-sdk-go/aws"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	// "net"
	// "net/http"
	"os"
	// "time"
)


var (
	// Provided by amazon.
	AWSRegion           = os.Getenv("AWS_REGION")
	UploadConcurrency   = 5
	DownloadConcurrency = 10
	awssess             *awsSession.Session
)

func AWSSession() *awsSession.Session {
	if awssess == nil {
		// transport := &http.Transport{
		// 	Dial: (&net.Dialer{
		// 		Timeout:       30 * time.Second,
		// 		KeepAlive:     60 * time.Second,
		// 	}).Dial,
		// 	DisableCompression:  false,
		// 	DisableKeepAlives:   false,
		// 	MaxIdleConns:        Concurrency,
		// 	MaxIdleConnsPerHost: Concurrency,
		// 	IdleConnTimeout:     15 * time.Minute,  // Base on max lambda lifespan.
		// }
		// client := &http.Client{Timeout: 0, Transport: transport}
		awssess = awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
			SharedConfigState: awsSession.SharedConfigEnable,
			Config:            aws.Config{
				// HTTPClient: client,
				// DisableSSL: aws.Bool(true),
				Region: aws.String(AWSRegion)},
		}))
	}
	return awssess
}
