package main

import (
	"os"
	"time"
)

const (
	LIFESPAN = 5 * time.Minute      // Not effective for now
	MIN_TICK = 1 * time.Millisecond // Set to 100ms to compare with legacy system.
)

var (
	// Bucket to store persistent data. Keep "%s" at the end of the bucket name.
	S3_BACKUP_BUCKET string = "infinistore.backup%s"
	// Bucket to store experiment data. No date will be stored if InputEvent.Prefix is not set.
	S3_COLLECTOR_BUCKET string = "ds2-lab.datapool"

	DRY_RUN = true
)

func init() {
	// Overwrite if environment variables are set.
	S3_BACKUP_BUCKET = GetenvIf(os.Getenv("S3_BACKUP_BUCKET"), S3_BACKUP_BUCKET)
	S3_COLLECTOR_BUCKET = GetenvIf(os.Getenv("S3_COLLECTOR_BUCKET"), S3_COLLECTOR_BUCKET)
}

func GetenvIf(env string, def string) string {
	if len(env) > 0 {
		return env
	} else {
		return def
	}
}
