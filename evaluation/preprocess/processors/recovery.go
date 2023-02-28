package processors

import (
	"io"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/ds2-lab/infinistore/evaluation/preprocess/options"
)

var (
	recoveryFilenameMatcher = regexp.MustCompile(`^\d+$`)
)

func init() {
	registry["recovery"] = &RecoveryPreprocessor{}
}

type RecoveryPreprocessor struct {
	abstractPreprocessor
}

func (p *RecoveryPreprocessor) WriteTitle(f io.Writer, opts *options.Options) {
	io.WriteString(f, "no,mem,numbackups,objsize,interval,time,op,recovery,node,backey,lineage,objects,total,lineagesize,objectsize,numobjects,session\n")
}

// Configure
func (p *RecoveryPreprocessor) Configure(opts *options.Options) error {
	if opts.FileFilter == "" && opts.Processor == "recovery" {
		opts.FileMatcher = recoveryFilenameMatcher
	}
	return nil
}

func (p *RecoveryPreprocessor) ParseContent(df string, file io.Writer, prepend string, opts *options.Options) error {
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
		fields := strings.Split(line, ",")
		for len(fields) >= 12 {
			l := strings.Join(fields[:11], ",")
			if len(prepend) > 0 {
				io.WriteString(file, prepend)
				io.WriteString(file, ",")
			}
			io.WriteString(file, l)
			io.WriteString(file, ",")
			if len(fields[11]) > 36 {
				io.WriteString(file, fields[11][:36])
				io.WriteString(file, "\n")
				fields[10] = fields[11][36:]
				fields = fields[11:]
			} else {
				io.WriteString(file, fields[11])
				io.WriteString(file, "\n")
				fields = fields[12:]
			}
		}
		if len(fields) > 0 {
			log.Warn("Unexepected remains in %s: %v", df, fields)
		}
	}
	return nil
}
