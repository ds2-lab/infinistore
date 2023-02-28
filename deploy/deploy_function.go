package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/mason-leap-lab/go-utils/config"
)

const (
	ENV_COS_BUCKET  string = "S3_BACKUP_BUCKET"
	ENV_DATA_BUCKET string = "S3_COLLECTOR_BUCKET"
)

type Options struct {
	config.LoggerOptions

	// Configurable in config.yml
	Region         string `name:"region" desc:"AWS region, change it if necessary."`
	Role           string `name:"aws-iam-role" desc:"ARN of your AWS role, which has the proper policy (AWSLambdaFullAccess, AWSLambdaVPCAccessExecutionRole, and AWSLambdaENIManagementAccess are required, see README.md for details)."`
	Subnets        string `name:"aws-vpc-subnets" desc:"Subnets' ID for AWS Lambda, comma separated."`
	SecurityGroups string `name:"aws-security-groups" desc:"Security groups' ID for AWS EC2, comma separated."`
	COSBucket      string `name:"s3-bucket-cos" desc:"S3 bucket name for InfiniStore COS layer."`
	DataBucket     string `name:"s3-bucket-data" desc:"S3 bucket name for collecting data originated from AWS Lambda."`

	// Configurable in shell scripts
	LambdaBucket string `name:"s3-bucket-lambda" desc:"S3 bucket name for uploading the binary of AWS Lambda."`
	Create       bool   `name:"create" desc:"Instruct the AWS Lambda should be created."`
	UpdateCode   bool   `name:"code" desc:"Instruct the binary of AWS Lambda should be updated."`
	UpdateConfig bool   `name:"config" desc:"Instruct the configuration of AWS Lambda should be updated."`
	LambdaKey    string `name:"key" desc:"Key for Lambda handler and file name."`
	VPC          bool   `name:"vpc" desc:"Instruct the AWS Lambda should be created with VPC."`
	Prefix       string `name:"prefix" desc:"Prefix of AWS Lambda function name."`
	UpdateFrom   int64  `name:"from" desc:"The start of the postfix of AWS Lambda function name."`
	UpdateTo     int64  `name:"to" desc:"The end of the postfix of AWS Lambda function name."`
	Batch        int64  `name:"batch" desc:"The number of concurrent AWS update job, no need to modify."`

	// AWS Lambda specification. Configurable in shell scripts.
	MemorySize int64 `name:"mem" desc:"The memory of AWS Lambda."`
	Timeout    int64 `name:"timeout" desc:"The timeout of AWS Lambda."`

	// Internal
	subnets        []*string
	securityGroups []*string
	envs           map[string]*string
}

var (
	// Assign default values to options.
	options = &Options{
		LambdaKey:  "lambda",
		Prefix:     "MemoryNode",
		UpdateFrom: 0,
		UpdateTo:   1000,
		Batch:      5,
		MemorySize: 1024,
		Timeout:    600,
	}
)

func updateConfig(name string, svc *lambda.Lambda, wg *sync.WaitGroup) {
	var vpcConfig *lambda.VpcConfig
	if options.VPC {
		vpcConfig = &lambda.VpcConfig{SubnetIds: options.subnets, SecurityGroupIds: options.securityGroups}
	} else {
		vpcConfig = &lambda.VpcConfig{}
	}
	input := &lambda.UpdateFunctionConfigurationInput{
		//Description:  aws.String(""),
		FunctionName: aws.String(name),
		Handler:      aws.String(options.LambdaKey),
		MemorySize:   aws.Int64(options.MemorySize),
		Role:         aws.String(options.Role),
		Timeout:      aws.Int64(options.Timeout),
		VpcConfig:    vpcConfig,
		Environment:  &lambda.Environment{Variables: options.envs},
	}
	result, err := svc.UpdateFunctionConfiguration(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case lambda.ErrCodeServiceException:
				fmt.Println(lambda.ErrCodeServiceException, aerr.Error())
			case lambda.ErrCodeResourceNotFoundException:
				fmt.Println(lambda.ErrCodeResourceNotFoundException, aerr.Error())
			case lambda.ErrCodeInvalidParameterValueException:
				fmt.Println(lambda.ErrCodeInvalidParameterValueException, aerr.Error())
			case lambda.ErrCodeTooManyRequestsException:
				fmt.Println(lambda.ErrCodeTooManyRequestsException, aerr.Error())
			case lambda.ErrCodeResourceConflictException:
				fmt.Println(lambda.ErrCodeResourceConflictException, aerr.Error())
			case lambda.ErrCodePreconditionFailedException:
				fmt.Println(lambda.ErrCodePreconditionFailedException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}
	fmt.Println(name, "\n", result)
	wg.Done()
}

func updateCode(name string, svc *lambda.Lambda, wg *sync.WaitGroup) {
	input := &lambda.UpdateFunctionCodeInput{
		FunctionName: aws.String(name),
		S3Bucket:     aws.String(options.LambdaBucket),
		S3Key:        aws.String(fmt.Sprintf("%s.zip", options.LambdaKey)),
	}
	result, err := svc.UpdateFunctionCode(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case lambda.ErrCodeServiceException:
				fmt.Println(lambda.ErrCodeServiceException, aerr.Error())
			case lambda.ErrCodeResourceNotFoundException:
				fmt.Println(lambda.ErrCodeResourceNotFoundException, aerr.Error())
			case lambda.ErrCodeInvalidParameterValueException:
				fmt.Println(lambda.ErrCodeInvalidParameterValueException, aerr.Error())
			case lambda.ErrCodeTooManyRequestsException:
				fmt.Println(lambda.ErrCodeTooManyRequestsException, aerr.Error())
			case lambda.ErrCodeCodeStorageExceededException:
				fmt.Println(lambda.ErrCodeCodeStorageExceededException, aerr.Error())
			case lambda.ErrCodePreconditionFailedException:
				fmt.Println(lambda.ErrCodePreconditionFailedException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}
	fmt.Println(name, "\n", result)
	wg.Done()
}

func createFunction(name string, svc *lambda.Lambda) {
	fmt.Println("create:", name)
	var vpcConfig *lambda.VpcConfig
	if options.VPC {
		vpcConfig = &lambda.VpcConfig{SubnetIds: options.subnets, SecurityGroupIds: options.securityGroups}
	} else {
		vpcConfig = &lambda.VpcConfig{}
	}
	input := &lambda.CreateFunctionInput{
		Code: &lambda.FunctionCode{
			S3Bucket: aws.String(options.LambdaBucket),
			S3Key:    aws.String(fmt.Sprintf("%s.zip", options.LambdaKey)),
		},
		FunctionName: aws.String(name),
		Handler:      aws.String(options.LambdaKey),
		MemorySize:   aws.Int64(options.MemorySize),
		Role:         aws.String(options.Role),
		Runtime:      aws.String("go1.x"),
		Timeout:      aws.Int64(options.Timeout),
		VpcConfig:    vpcConfig,
		Environment: &lambda.Environment{
			Variables: options.envs,
		},
	}

	result, err := svc.CreateFunction(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case lambda.ErrCodeServiceException:
				fmt.Println(lambda.ErrCodeServiceException, aerr.Error())
			case lambda.ErrCodeInvalidParameterValueException:
				fmt.Println(lambda.ErrCodeInvalidParameterValueException, aerr.Error())
			case lambda.ErrCodeResourceNotFoundException:
				fmt.Println(lambda.ErrCodeResourceNotFoundException, aerr.Error())
			case lambda.ErrCodeResourceConflictException:
				fmt.Println(lambda.ErrCodeResourceConflictException, aerr.Error())
			case lambda.ErrCodeTooManyRequestsException:
				fmt.Println(lambda.ErrCodeTooManyRequestsException, aerr.Error())
			case lambda.ErrCodeCodeStorageExceededException:
				fmt.Println(lambda.ErrCodeCodeStorageExceededException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(name, '\n', result)
	//wg.Done()
}

//func upload(sess *session.Session) {
//	// Create an uploader with the session and default options
//	uploader := s3manager.NewUploader(sess)
//
//	f, err := os.Open(fileName)
//	if err != nil {
//		fmt.Println("failed to open file", fileName, err)
//	}
//
//	// Upload the file to S3.
//	result, err := uploader.Upload(&s3manager.UploadInput{
//		Bucket: aws.String(BUCKET),
//		Key:    aws.String(KEY),
//		Body:   f,
//	})
//	if err != nil {
//		fmt.Println("failed to upload file", err)
//	}
//	fmt.Println("file uploaded to", result.Location)
//}

func main() {
	flags, err := config.ValidateOptions(options)
	if err == config.ErrPrintUsage {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}
	// normalize options
	subnets := strings.Split(options.Subnets, ",")
	options.subnets = make([]*string, len(subnets))
	for i := range subnets {
		options.subnets[i] = aws.String(subnets[i])
	}
	securityGroups := strings.Split(options.SecurityGroups, ",")
	options.securityGroups = make([]*string, len(securityGroups))
	for i := range securityGroups {
		options.securityGroups[i] = aws.String(securityGroups[i])
	}
	options.envs = make(map[string]*string)
	if options.COSBucket != "" {
		options.envs[ENV_COS_BUCKET] = aws.String(options.COSBucket)
	}
	if options.DataBucket != "" {
		options.envs[ENV_DATA_BUCKET] = aws.String(options.DataBucket)
	}

	// get group count
	group := int64(math.Ceil(float64(options.UpdateTo-options.UpdateFrom) / float64(options.Batch)))
	//fmt.Println("group", group)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := lambda.New(sess, &aws.Config{Region: aws.String(options.Region)})

	if options.Create {
		for i := options.UpdateFrom; i < options.UpdateTo; i++ {
			createFunction(fmt.Sprintf("%s%d", options.Prefix, i), svc)
		}
	}

	if options.UpdateCode {
		for j := int64(0); j < group; j++ {
			fmt.Println(j)
			var wg sync.WaitGroup
			//for i := j*(*batch) + *from; i < (j+1)*(*batch); i++ {
			for i := int64(0); i < options.Batch && j*(options.Batch)+options.UpdateFrom+i < options.UpdateTo; i++ {
				wg.Add(1)
				go updateCode(fmt.Sprintf("%s%d", options.Prefix, j*(options.Batch)+options.UpdateFrom+i), svc, &wg)
			}
			wg.Wait()
			time.Sleep(1 * time.Second)
		}
	}

	if options.UpdateConfig {
		for j := int64(0); j < group; j++ {
			fmt.Println(j)
			var wg sync.WaitGroup
			for i := int64(0); i < options.Batch && j*(options.Batch)+options.UpdateFrom+i < options.UpdateTo; i++ {
				wg.Add(1)
				go updateConfig(fmt.Sprintf("%s%d", options.Prefix, j*(options.Batch)+options.UpdateFrom+i), svc, &wg)
			}
			wg.Wait()
			time.Sleep(1 * time.Second)
		}
	}
}
