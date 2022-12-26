package processors

import (
	"io"
	"io/ioutil"
	"strings"

	"github.com/mason-leap-lab/infinicache/evaluation/preprocess/options"
)

type CsvPreprocessor struct {
	abstractPreprocessor
}

func (p *CsvPreprocessor) ParseContent(df string, file io.Writer, prepend string, opts *options.Options) error {
	dfile, err := ioutil.ReadFile(df)
	if err != nil {
		return err
	}
	// Normalize
	lines := strings.Split(string(dfile), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		io.WriteString(file, prepend)
		io.WriteString(file, ",")
		io.WriteString(file, line)
		io.WriteString(file, "\n")
	}
	return nil
}

func init() {
	registry["csv"] = &CsvPreprocessor{}
}
