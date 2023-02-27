package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"

	"github.com/ds2-lab/infinistore/evaluation/preprocess/options"
	"github.com/ds2-lab/infinistore/evaluation/preprocess/processors"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

var (
	log logger.Logger = logger.NilLogger
)

func main() {
	opts := &options.Options{
		Output:    "exp.csv",
		Processor: "csv",
	}
	flags, err := config.ValidateOptions(opts)
	// Ensure path specified.
	if err == nil && flags.NArg() < 1 {
		err = config.ErrPrintUsage
	} else {
		opts.Path = flags.Arg(0)
	}
	// Check usage.
	if err == config.ErrPrintUsage {
		fmt.Fprintf(os.Stderr, "Usage: ./preprocess [options] data_base_path\n")
		fmt.Fprintf(os.Stderr, "Available options:\n")
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		panic(err)
	}

	log = config.GetDefaultLogger()

	oFlags := os.O_CREATE | os.O_WRONLY
	if opts.Merge {
		oFlags |= os.O_APPEND
	} else {
		oFlags |= os.O_TRUNC
	}
	file, err := os.OpenFile(opts.Output, oFlags, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	all := make(chan string, 1)
	go func() {
		root, err := os.Stat(opts.Path)
		if err != nil {
			log.Error("Error on get stat %s: %v", opts.Path, err)
		} else if !root.IsDir() {
			log.Info("Collecting %s", opts.Path)
			all <- opts.Path
		} else if err := iterateDir(opts.Path, opts.FileMatcher, all); err != nil {
			log.Error("Error on iterating path: %v", err)
		}

		close(all)
	}()

	processor, ok := processors.LoadProcessor(opts.Processor)
	if !ok {
		log.Error("Unsupported processor: %s", opts.Processor)
		return
	}
	if err := processor.Configure(opts); err != nil {
		log.Error("Error on configuring processor: %v", err)
		return
	}
	collectAll(processor, all, file, opts)
}

func iterateDir(root string, filter *regexp.Regexp, collectors ...chan string) error {
	log.Debug("Reading %s", root)
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return err
	}

	for _, file := range files {
		full := path.Join(root, file.Name())
		if file.IsDir() {
			iterateDir(full, filter, collectors...)
		} else if filter == nil || filter.MatchString(file.Name()) {
			log.Info("Collecting %s", full)
			for _, collector := range collectors {
				collector <- full
			}
		}
	}

	return nil
}

func collectAll(processor processors.Preprocessor, dataFiles chan string, file io.Writer, opts *options.Options) {
	if !opts.Merge {
		processor.WriteTitle(file, opts)
	}
	for df := range dataFiles {
		prepend, err := processor.ParseFilename(df)
		if err != nil {
			log.Error("Error on parse the \"%s\": %v", df, err)
			continue
		}
		if err := processor.ParseContent(df, file, prepend, opts); err != nil {
			log.Warn("Failed to process %s: %v", df, err)
		}
	}
}
