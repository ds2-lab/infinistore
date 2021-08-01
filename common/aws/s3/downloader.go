package s3

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var ()

// S3 Downloader Optimization
// Because the minimum concurrent download unit for aws download api is 5M,
// this optimization will effectively lower this limit for batch downloading
type Downloader struct {
	backends []*s3manager.Downloader
	queue    chan *BatchDownloadObject
	pool     *sync.Pool

	Concurrency    int
	BufferProvider s3manager.WriterReadFromProvider
	RequestOptions []request.Option
}

// Customized version of s3manager.BatchDownloadObject
type BatchDownloadObject struct {
	Object     *s3.GetObjectInput
	Size       uint64
	Writer     io.WriterAt
	After      func() error
	Meta       interface{}
	Downloaded int64
	Error      error
}

func (bdo *BatchDownloadObject) Bytes() []byte {
	if bdo.Writer != nil {
		return bdo.Writer.(*aws.WriteAtBuffer).Bytes()
	}
	return nil
}

func NewDownloader(sess *session.Session, options ...func(*Downloader)) *Downloader {
	downloader := &Downloader{
		Concurrency: 5,
	}
	for _, option := range options {
		option(downloader)
	}

	if downloader.Concurrency < 1 {
		downloader.Concurrency = 1
	}
	downloader.backends = make([]*s3manager.Downloader, downloader.Concurrency)
	downloader.queue = make(chan *BatchDownloadObject, downloader.Concurrency)
	downloader.pool = &sync.Pool{
		New: func() interface{} {
			return &BatchDownloadObject{
				Object: &s3.GetObjectInput{},
			}
		},
	}

	// This is essential to minimize download memory consumption.
	if downloader.BufferProvider == nil {
		downloader.BufferProvider = NewPooledBufferedWriterReadFromProvider(0)
	}

	// Initialize backend downloaders
	for i := 0; i < len(downloader.backends); i++ {
		downloader.backends[i] = s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
			d.Concurrency = 1
			d.BufferProvider = downloader.BufferProvider
			d.RequestOptions = downloader.RequestOptions
		})
	}
	return downloader
}

func (d *Downloader) Close() {
	for i := 0; i < len(d.backends); i++ {
		d.backends[i] = nil
	}
	d.BufferProvider.(*PooledBufferedReadFromProvider).Close()
	d.BufferProvider = nil
	d.pool = nil
	d.queue = nil
	d.backends = nil
}

func (d *Downloader) GetDownloadPartSize() uint64 {
	if len(d.backends) > 0 {
		return uint64(d.backends[0].PartSize)
	} else {
		return uint64(s3manager.DefaultDownloadPartSize)
	}
}

func (d *Downloader) Schedule(iter chan *BatchDownloadObject, builder func(*BatchDownloadObject)) (int, error) {
	input, err := d.build(builder)
	if err != nil {
		return 0, err
	}

	return d.schedule(iter, input, builder)
}

func (d *Downloader) Done(input *BatchDownloadObject) {
	d.pool.Put(input)
}

func (d *Downloader) build(builder func(*BatchDownloadObject)) (*BatchDownloadObject, error) {
	input := d.pool.Get().(*BatchDownloadObject)
	input.Object.Bucket = nil
	input.Object.Key = nil
	input.Object.Range = nil
	input.Size = 0
	input.Writer = nil
	input.After = nil
	input.Meta = nil
	input.Downloaded = 0
	input.Error = nil
	builder(input)

	if input.Size == 0 {
		return input, errors.New("the field Size must be set on building BatchDownloadObject")
	}
	if input.Writer == nil {
		input.Writer = aws.NewWriteAtBuffer(make([]byte, input.Size))
	}

	return input, nil
}

func (d *Downloader) schedule(iter chan *BatchDownloadObject, input *BatchDownloadObject, builder func(*BatchDownloadObject)) (int, error) {
	if input.Size <= d.GetDownloadPartSize() {
		iter <- input
		return 1, nil
	} else {
		offset := uint64(0)
		parts := math.Ceil(float64(input.Size) / float64(d.GetDownloadPartSize()))
		partSize := uint64(math.Ceil(float64(input.Size) / parts))
		for offset < input.Size {
			end := offset + Uint64Min(partSize, input.Size-offset)
			part := d.pool.Get().(*BatchDownloadObject)
			*part.Object = *input.Object
			part.Object.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, end-1))
			part.Size = end - offset
			part.Writer = aws.NewWriteAtBuffer(input.Bytes()[offset:end])
			part.After = input.After
			part.Meta = input.Meta
			builder(part)
			iter <- part

			offset = end
		}

		d.pool.Put(input)
		return int(parts), nil
	}
}

func (d *Downloader) Download(ctx aws.Context, builder func(*BatchDownloadObject)) error {
	input, err := d.build(builder)
	if err != nil {
		return err
	}

	parts := int(math.Ceil(float64(input.Size) / float64(d.GetDownloadPartSize())))
	iter := make(chan *BatchDownloadObject, parts)
	d.schedule(iter, input, builder)
	close(iter)

	backends := d.backends
	if parts < d.Concurrency {
		backends = d.backends[:parts]
	}
	return d.downloadWithIterator(ctx, iter, backends)
}

func (d *Downloader) DownloadWithIterator(ctx aws.Context, iter chan *BatchDownloadObject) error {
	return d.downloadWithIterator(ctx, iter, d.backends)
}

func (d *Downloader) downloadWithIterator(ctx aws.Context, iter chan *BatchDownloadObject, backends []*s3manager.Downloader) error {
	var wg sync.WaitGroup
	var errs []s3manager.Error
	chanErr := make(chan s3manager.Error, len(d.backends))

	// Launch object downloaders
	for i := 0; i < len(backends); i++ {
		wg.Add(1) // Added: download thread
		go d.download(ctx, backends[i], iter, chanErr, &wg)
	}

	// Collect errors
	go func() {
		for err := range chanErr {
			errs = append(errs, err)
		}
		wg.Done() // Done: close chanErr
	}()

	wg.Wait()

	// Wait for chanErr
	wg.Add(1) // Added: close chanErr
	close(chanErr)
	wg.Wait()

	if len(errs) > 0 {
		return s3manager.NewBatchError("BatchedDownloadIncomplete", "some objects have failed to download.", errs)
	}
	return nil
}

func (d *Downloader) download(ctx aws.Context, downloader *s3manager.Downloader, ch chan *BatchDownloadObject, errs chan s3manager.Error, wg *sync.WaitGroup) {
	defer wg.Done() // Done: download thread

	// log := ctx.Value(&ContextKeyLog).(logger.ILogger)
	for object := range ch {
		// if object.Object.Range != nil {
		// 	log.Debug("Downloading: %s(%s)...", *object.Object.Key, *object.Object.Range)
		// } else {
		// 	log.Debug("Downloading: %s...", *object.Object.Key)
		// }

		if object.Downloaded, object.Error = downloader.Download(object.Writer, object.Object); object.Error != nil {
			// log.Warn("%v", object.Error)
			errs <- s3manager.Error{OrigErr: object.Error, Bucket: object.Object.Bucket, Key: object.Object.Key}
		}
		// if object.Object.Range != nil {
		// 	log.Debug("Finished: %s(%s)", *object.Object.Key, *object.Object.Range)
		// } else {
		// 	log.Debug("Finished: %s", *object.Object.Key)
		// }

		if object.After == nil {
			continue
		}

		if object.Error = object.After(); object.Error != nil {
			errs <- s3manager.Error{OrigErr: object.Error, Bucket: object.Object.Bucket, Key: object.Object.Key}
		}
	}
}

func Uint64Min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func Uint64Max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}
