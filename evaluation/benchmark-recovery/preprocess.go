package main

import (
	"flag"
	"fmt"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
)

var (
	log = &logger.ColorLogger{ Color: true, Level: logger.LOG_LEVEL_INFO }

	// No.1_lambda2048_40_100_500
	FilenameMatcher = regexp.MustCompile(`^\d+$`)
	FileNameRecognizer = regexp.MustCompile(`^No.(\d+)_lambda(\d+)_(\d+)_(\d+)_(\d+)$`)
	NodeRecognizer = regexp.MustCompile(`^Store1VPCNode0(\d+)$`)
	MainNode = 0
	RecoveryNodes = 12
)

type Options struct {
	path       string
	output     string
	merge      bool
}

func main() {
	options := &Options{}
	checkUsage(options)

	all := make(chan string, 1)
	if err := collectAll(options.output, all, options); err != nil {
		log.Error("%v", all)
		os.Exit(1);
	}

	if err := iterateDir(options.path, FilenameMatcher, all); err != nil {
		log.Error("Error on iterating path: %v", err)
	}

	close(all)
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
		} else if filter.MatchString(file.Name()){
			log.Debug("Collecting %s", full)
			for _, collector := range collectors {
				collector <- full
			}
		}
	}

	return nil
}

func collectAll(output string, dataFiles chan string, opts *Options) error {
	flags := os.O_CREATE|os.O_WRONLY
	if opts.merge {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(output, flags, 0644)
	if err != nil {
		return err
	}

	go func() {
		defer file.Close()

		writeTitle(file)
		for df := range dataFiles {
			file.WriteString(strings.Join(FileNameRecognizer.FindStringSubmatch(df), ","))
			file.WriteString(",")

			dfile, err := ioutil.ReadFile(df)
			if err != nil {
				log.Warn("Failed to read %s: %v", df, err)
				continue
			}
			file.WriteString(string(dfile))
			file.WriteString("\n")
		}
	}()

	return nil
}

func writeTitle(f *os.File) {
	f.WriteString("no,mem,numbackups,objsize,interval,op,recovery,node,backey,lineage,objects,total,lineagesize,objectsize,numobjects,session\n")
}

func checkUsage(options *Options) {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	flag.StringVar(&options.output, "o", "exp.csv", "Filename of merged data.")
	flag.BoolVar(&options.merge, "a", false, "Append to output if possible.")

	flag.Parse()

	if printInfo || flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: ./preprocess [options] data_base_path\n")
		fmt.Fprintf(os.Stderr, "Available options:\n")
		flag.PrintDefaults()
		os.Exit(0);
	}

	options.path = flag.Arg(0)
}
