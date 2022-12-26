package processors

import (
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/evaluation/preprocess/options"
)

var (
	registry = make(map[string]Preprocessor)
	log      = &logger.ColorLogger{Color: true, Level: logger.LOG_LEVEL_ALL, Prefix: "Processor "}

	// Microbench filenames are like: No.1_lambda2048_40_100_500
	fileNameRecognizer = regexp.MustCompile(`No.(\d+)_lambda(\d+)_(\d+)_(\d+)_(\d+)`)
)

type Preprocessor interface {
	// Configure configures the preprocessor with options.
	Configure(opts *options.Options) error

	// WriteTitle writes the title of the output file.
	WriteTitle(f io.Writer, opts *options.Options)

	// ParseFilename parse filename and returns a string that conctains values common separated.
	ParseFilename(filename string) (string, error)

	ParseContent(df string, file io.Writer, prepend string, opts *options.Options) error
}

func LoadProcessor(name string) (Preprocessor, bool) {
	porcess, ok := registry[name]
	return porcess, ok
}

type abstractPreprocessor struct {
}

func (p *abstractPreprocessor) WriteTitle(f io.Writer, opts *options.Options) {
}

func (p *abstractPreprocessor) Configure(opts *options.Options) error {
	return nil
}

func (p *abstractPreprocessor) ParseFilename(filename string) (string, error) {
	values := fileNameRecognizer.FindStringSubmatch(filename)
	if len(values) == 0 {
		return "", fmt.Errorf("unexpected filename for %v", fileNameRecognizer)
	}
	return strings.Join(values[1:], ","), nil
}
