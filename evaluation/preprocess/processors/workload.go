package processors

import (
	"fmt"
	"io"
	"regexp"

	"github.com/mason-leap-lab/infinicache/evaluation/preprocess/options"
)

func init() {
	registry["workload"] = &WorkloadPreprocessor{}
}

type WorkloadPreprocessor struct {
	RecoveryPreprocessor

	functionRecognizer *regexp.Regexp
}

func (p *WorkloadPreprocessor) WriteTitle(f io.Writer, opts *options.Options) {
	io.WriteString(f, "func,time,op,recovery,node,backey,lineage,objects,total,lineagesize,objectsize,numobjects,session\n")
}

func (p *WorkloadPreprocessor) Configure(opts *options.Options) error {
	if opts.FunctionPrefix != "" {
		p.functionRecognizer = regexp.MustCompile(fmt.Sprintf("%s(\\d+)", opts.FunctionPrefix))
		log.Info("Will recognize function name like %v", p.functionRecognizer)
	}
	return nil
}

// ParseFilename
func (p *WorkloadPreprocessor) ParseFilename(filename string) (string, error) {
	// fi, err := os.Stat(df)
	// if err != nil {
	// 	log.Warn("Failed to get stat of file %s: %v", df, err)
	// 	continue
	// }
	// stat_t := fi.Sys().(*syscall.Stat_t)
	// ct := time.Unix(int64(stat_t.Mtimespec.Sec), int64(0)) // Mac
	// ct := time.Unix(int64(stat_t.Ctim.Sec), int64(0)) // Linux
	// prepend := ct.UTC().String()
	prepend := ""
	if p.functionRecognizer != nil {
		matched := p.functionRecognizer.FindStringSubmatch(filename)
		if len(matched) > 0 {
			prepend = matched[0]
		}
	}
	return prepend, nil
}
