package main

import (
	"os"
	"regexp"
	"time"
)

const (
	LIFESPAN = 5 * time.Minute      // Not effective for now
	MIN_TICK = 1 * time.Millisecond // Set to 100ms to compare with legacy system.
)

var (
	// Bucket to store persistent data.
	S3_BACKUP_BUCKET string = "infinistore.backup%s"
	// Bucket to store experiment data. No date will be stored if InputEvent.Prefix is not set.
	S3_COLLECTOR_BUCKET string = "ds2-lab.datapool"

	DRY_RUN = false
)

func init() {
	// Overwrite if environment variables are set.
	S3_BACKUP_BUCKET = GetenvIf(os.Getenv("S3_BACKUP_BUCKET"), S3_BACKUP_BUCKET)
	S3_COLLECTOR_BUCKET = GetenvIf(os.Getenv("S3_COLLECTOR_BUCKET"), S3_COLLECTOR_BUCKET)

	// Normalize backup bucket name with a pending %s.
	S3_BACKUP_BUCKET = regexp.MustCompile(`^(.*?)(%s)?$`).ReplaceAllString(S3_BACKUP_BUCKET, "$1%s")
}

func GetenvIf(env string, def string) string {
	if len(env) > 0 {
		return env
	} else {
		return def
	}
}
