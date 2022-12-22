package main

import (
	"flag"
	"log"
	"os"

	"github.com/mason-leap-lab/go-utils/config"
)

type Options struct {
	config.LoggerOptions
}

func main() {
	var options Options
	flags, err := config.ValidateOptions(&options)
	if err == config.ErrPrintUsage {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}

	if flags.NArg() < 2 {
		log.Fatal("No command specified.")
	}

	cmd := flags.Arg(1)
	switch cmd {
	case "summary":
		ParseSummary(flags, &options)
	default:
		log.Fatal("Unknown command: ", cmd)
	}
}

func ParseSummary(flags *flag.FlagSet, options *Options) {
	if flags.NArg() < 3 {
		log.Fatal("No file specified.")
	}

	file := flags.Arg(2)
	summary, err := ParseSummaryFile(file)
	if err != nil {
		log.Fatal(err)
	}

	summary.Print()
}
