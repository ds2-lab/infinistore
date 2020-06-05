package metastore

const (
	// Metastore events start from 300

	// Report there's not enough space in storage
	// Parameter: *lambdastore.Instance
	EventInsufficientStorage = 300
)
