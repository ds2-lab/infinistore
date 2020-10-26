package types

import (
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
)

var (
	// Provided by amazon.
	AWSRegion           = os.Getenv("AWS_REGION")
	UploadConcurrency   = 5
	DownloadConcurrency = 10
	// AWSDummyTimeout     = 10 * time.Millisecond
	AWSServiceTimeout = 10000 * time.Millisecond

	awssess *awsSession.Session
)

func AWSSession() *awsSession.Session {
	if awssess == nil {
		transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: AWSServiceTimeout,
				// KeepAlive:     60 * time.Second,       // Interval for keep-alive probe. Default 15s.
			}).DialContext,
			// DisableCompression:  false,
			// DisableKeepAlives:   false,
			// MaxIdleConns:        DownloadConcurrency,
			MaxIdleConnsPerHost: 1,
			// MaxConnsPerHost:     DownloadConcurrency,
			ResponseHeaderTimeout: AWSServiceTimeout,
		}
		client := &http.Client{Timeout: 0, Transport: transport}
		awssess = awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
			SharedConfigState: awsSession.SharedConfigEnable,
			Config: aws.Config{
				HTTPClient: client,
				DisableSSL: aws.Bool(true),
				Region:     aws.String(AWSRegion)},
		}))
	}
	return awssess
}
