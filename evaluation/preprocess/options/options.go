package options

import (
	"regexp"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

var (
	log = &logger.ColorLogger{Color: true, Level: logger.LOG_LEVEL_INFO}
)

// Options Options definition
type Options struct {
	config.LoggerOptions

	Path            string
	Output          string `name:"o" description:"Filename of merged data."`
	Merge           bool   `name:"a" description:"Append to output if possible."`
	Processor       string `name:"processor" description:"Function to process one file: csv, nanolog, recovery, analytics, workload, ycsb"`
	FileFilter      string `name:"filter-files" description:"Regexp to filter files to process."`
	FileMatcher     *regexp.Regexp
	LineFilter      string `name:"filter-lines" description:"Regexp to filter lines to output."`
	LineMatcher     *regexp.Regexp
	LastLineFilter  string `name:"filter-previous-line" description:"Regexp to filter lines that has specified previous line to output."`
	LastLineMatcher *regexp.Regexp
	FunctionPrefix  string `name:"fprefix" description:"Regexp for function recognition."`
	YCSBFields      string `name:"ycsb-fields" description:"Fields to output for YCSB workload in format \"category1.field1->rename1[,category2.field2->rename2]\"."`
}

// Validate validates options
func (opts *Options) Validate() error {
	if opts.FileFilter != "" {
		opts.FileMatcher = regexp.MustCompile(opts.FileFilter)
		log.Info("Will filter files matching %v", opts.FileMatcher)
	} else if opts.LineFilter != "" {
		opts.LineMatcher = regexp.MustCompile(opts.LineFilter)
		log.Info("Will filter lines matching %v", opts.LineMatcher)
	}

	if opts.LastLineFilter != "" {
		opts.LastLineMatcher = regexp.MustCompile(opts.LastLineFilter)
		log.Info("Will filter last line matching %v", opts.LastLineMatcher)
	}
	return nil
}
