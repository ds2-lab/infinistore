package main

import (
	"os"
	"time"
)

const (
	LIFESPAN            = 5 * time.Minute
)

var (
	AWS_REGION string
	S3_COLLECTOR_BUCKET string      = "infinicache.collector"
	S3_BACKUP_BUCKET string         = "infinicache.backup"
)

func init() {
	// Provided by amazon.
	AWS_REGION = os.Getenv("AWS_REGION")

	// Set required
	S3_COLLECTOR_BUCKET = GetenvIf(os.Getenv("S3_COLLECTOR_BUCKET"), S3_COLLECTOR_BUCKET)

	// Set required
	S3_BACKUP_BUCKET = GetenvIf(os.Getenv("S3_BACKUP_BUCKET"), S3_BACKUP_BUCKET)
}

func GetenvIf(env string, def string) string {
	if len(env) > 0 {
		return env
	} else {
		return def
	}
}
