package processors

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/ds2-lab/infinistore/evaluation/preprocess/options"
)

var (
	analyticsTaskRecognizer = regexp.MustCompile(`(map|reduce)_io_data_([a-zA-Z]+)\d+\.dat`)
	// analyticsLineRecognizer = regexp.MustCompile(`^\{(.+?)\}$`)
)

type DataAnlyticsPreprocessor struct {
	abstractPreprocessor
}

func (p *DataAnlyticsPreprocessor) WriteTitle(f io.Writer, opts *options.Options) {
	io.WriteString(f, "op,task,taskid,taskkey,size,start,end\n")
}

// ParseFilename
func (p *DataAnlyticsPreprocessor) ParseFilename(filename string) (string, error) {
	matched := analyticsTaskRecognizer.FindStringSubmatch(filename)
	prepend := ""
	if len(matched) > 0 {
		prepend = strings.Join(matched[1:], ",")
	}
	return prepend, nil
}

func (p *DataAnlyticsPreprocessor) ParseContent(df string, file io.Writer, prepend string, opts *options.Options) error {
	dfile, err := os.Open(df)
	if err != nil {
		return err
	}
	defer dfile.Close()

	s := bufio.NewScanner(dfile)
	s.Split(bufio.ScanLines)
	for s.Scan() {
		line := s.Text()
		// matches := analyticsLineRecognizer.FindStringSubmatch(line)
		// if len(matches) == 0 {
		// 	continue
		// }
		// line = matches[1]
		line = line[1 : len(line)-1]

		io.WriteString(file, prepend)
		io.WriteString(file, ",")
		segments := strings.Split(line, " ")
		if len(segments) == 1 {
			segments = strings.Split(line, "\t")
		}
		io.WriteString(file, strings.Join(segments, ","))
		io.WriteString(file, "\n")
	}
	return nil
}

func init() {
	registry["analytics"] = &DataAnlyticsPreprocessor{}
}
