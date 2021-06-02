package types

import "time"

var (
	HeaderTimeout     = 1 * time.Second
	MinBodyTimeout    = 1 * time.Second
	BandwidthFactor   = int64(100) // 1/bandwidth, while bandwidth = 10MB/s = 0.01B/ns for single connection
	BodyTimeoutFactor = int64(5)
)

func GetDeadline(d time.Duration) time.Time {
	return time.Now().Add(d)
}

func GetHeaderTimeout() time.Duration {
	return HeaderTimeout
}

func GetHeaderDeadline() time.Time {
	return GetDeadline(HeaderTimeout)
}

func GetBodyTimeout(size int64) time.Duration {
	timeout := time.Duration(size * BandwidthFactor * BodyTimeoutFactor)
	if timeout < MinBodyTimeout {
		timeout = MinBodyTimeout
	}
	return timeout
}

func GetBodyDeadline(size int64) time.Time {
	return GetDeadline(GetBodyTimeout(size))
}
