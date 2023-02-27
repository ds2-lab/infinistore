package processors

import (
	"bufio"
	"io"
	"os"

	nanoReader "github.com/ScottMansfield/nanolog/reader"
	"github.com/ds2-lab/infinistore/evaluation/preprocess/options"
)

type NanologPreprocessor struct {
	abstractPreprocessor
}

func (p *NanologPreprocessor) ParseContent(df string, file io.Writer, prepend string, opts *options.Options) error {
	dfile, err := os.Open(df)
	if err != nil {
		return err
	}
	defer dfile.Close()

	reader, writer := io.Pipe()
	go func() {
		if err := nanoReader.New(dfile, writer).Inflate(); err != nil {
			log.Error("Failed to inflate %s: %v", df, err)
		}
		writer.Close()
	}()

	s := bufio.NewScanner(reader)
	s.Split(bufio.ScanLines)
	lastLine := ""
	for s.Scan() {
		line := s.Text()
		last := lastLine
		lastLine = line

		if len(line) == 0 {
			continue
		}
		if opts.LineMatcher != nil && !opts.LineMatcher.Match([]byte(line)) {
			continue
		}
		if opts.LastLineMatcher != nil && !opts.LastLineMatcher.Match([]byte(last)) {
			continue
		}
		if len(prepend) > 0 {
			io.WriteString(file, prepend)
			io.WriteString(file, ",")
		}
		io.WriteString(file, line)
		io.WriteString(file, "\n")
	}
	return nil
}

func init() {
	registry["nanolog"] = &NanologPreprocessor{}
}
