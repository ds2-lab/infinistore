package processors

import (
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/ds2-lab/infinistore/evaluation/preprocess/options"
)

var (
	// YCSB filenames are like: redisa_1K_c50_summary
	fileNameYCSBRecognizer = regexp.MustCompile(`(\S+?)(\S)_(\d+[KM])_c(\d+)_summary\.txt$`)

	lineYCSBMatcher = regexp.MustCompile(`^\[([A-Z_]+)\], ([^,]+), ([0-9.]+)$`)
)

func init() {
	registry["ycsb"] = &YCSBSummaryPreprocessor{}
}

type YCSBSummaryPreprocessor struct {
	outputFields []string
	definitions  []string
}

// Configure configures the preprocessor.
func (p *YCSBSummaryPreprocessor) Configure(opts *options.Options) error {
	if opts.FileMatcher == nil {
		opts.FileMatcher = fileNameYCSBRecognizer
	}
	opts.LineMatcher = lineYCSBMatcher

	if opts.YCSBFields == "" {
		return fmt.Errorf("no field definition specified")
	}
	fields := strings.Split(opts.YCSBFields, ",")
	if len(fields) == 0 {
		return fmt.Errorf("no field specified to output")
	}
	p.outputFields = make([]string, len(fields))
	p.definitions = make([]string, len(fields))
	for i, field := range fields {
		mapper := strings.Split(field, "->")
		if len(mapper) < 2 {
			return fmt.Errorf("invalid field definition: %v", field)
		}
		p.definitions[i] = mapper[0]
		p.outputFields[i] = mapper[1]
	}

	return nil
}

func (p *YCSBSummaryPreprocessor) WriteTitle(file io.Writer, opts *options.Options) {
	io.WriteString(file, "system,workload,size,concurrency")
	for _, field := range p.outputFields {
		io.WriteString(file, ",")
		io.WriteString(file, field)
	}
	io.WriteString(file, "\n")
}

// ParseFilename parse filename and returns a string that conctains values common separated.
func (p *YCSBSummaryPreprocessor) ParseFilename(filename string) (string, error) {
	values := fileNameYCSBRecognizer.FindStringSubmatch(filename)
	if len(values) == 0 {
		return "", fmt.Errorf("unexpected filename for %v", fileNameYCSBRecognizer)
	}
	values[1] = path.Base(values[1])
	if values[1] == "workload" {
		values[1] = "sion"
	}
	return strings.Join(values[1:], ","), nil
}

// ParseContent parses the content of the file and writes the result to the file.
func (p *YCSBSummaryPreprocessor) ParseContent(df string, file io.Writer, prepend string, opts *options.Options) error {
	dfile, err := ioutil.ReadFile(df)
	if err != nil {
		return err
	}
	// Normalize
	lines := strings.Split(string(dfile), "\n")
	values := make(map[string]float64)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		matches := opts.LineMatcher.FindStringSubmatch(line)
		if len(matches) == 0 {
			continue
		}
		val, _ := strconv.ParseFloat(matches[3], 64)
		values[fmt.Sprintf("%s.%s", matches[1], matches[2])] = val
	}

	io.WriteString(file, prepend)
	for _, def := range p.definitions {
		io.WriteString(file, ",")
		if val, ok := values[def]; !ok {
			io.WriteString(file, "0")
		} else if val/1 < 0.000001 {
			io.WriteString(file, strconv.FormatInt(int64(val), 10))
		} else {
			io.WriteString(file, strconv.FormatFloat(val, 'f', 6, 64))
		}
	}
	io.WriteString(file, "\n")
	return nil
}
