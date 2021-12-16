package global

import (
	"flag"
	"fmt"
	"os"
	"strings"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/config"
)

const (
	InvokerLocal = "local"
)

type CommandlineOptions struct {
	Pid         string
	Debug       bool
	Prefix      string
	PublicIP    string
	D           int
	P           int
	NoDashboard bool
	NoColor     bool
	LogPath     string
	LogFile     string
	Evaluation  bool
	NumBackups  int
	NoFirstD    bool

	lambdaPrefix       string
	funcCapacity       uint64
	funcChunkThreshold int
	disableRecovery    bool
	cluster            string
	numFunctions       int
	invoker            string

	// Profiling
	CpuProfile string
	MemProfile string
}

func (o *CommandlineOptions) GetLambdaPrefix() string {
	return o.lambdaPrefix
}

func (o *CommandlineOptions) GetClusterType() string {
	return strings.ToLower(o.cluster)
}

func (o *CommandlineOptions) GetNumFunctions() int {
	return o.numFunctions
}

func (o *CommandlineOptions) GetInstanceCapacity() uint64 {
	if o.funcCapacity == 0 {
		// Reset as default value
		o.funcCapacity = config.DefaultInstanceCapacity
	} else if o.funcCapacity < 128000000 {
		// Normalize to bytes
		o.funcCapacity = o.funcCapacity * 1000000
	}
	return o.funcCapacity
}

func (o *CommandlineOptions) GetInstanceChunkThreshold() int {
	if o.funcChunkThreshold == 0 {
		o.funcChunkThreshold = int(o.GetInstanceCapacity() / config.ChunkThreshold)
	}

	return o.funcChunkThreshold
}

func (o *CommandlineOptions) GetInvoker() string {
	return o.invoker
}

func CheckUsage(options *CommandlineOptions) {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	flag.BoolVar(&options.Debug, "debug", false, "Enable debug and print debug logs.")
	flag.StringVar(&options.Prefix, "prefix", "log", "Prefix for data files.")
	flag.StringVar(&options.lambdaPrefix, "lambda-prefix", "", "Prefix of the Lambda deployments.")
	flag.StringVar(&options.PublicIP, "ip", "", "Public IP for non-VPC Lambda deployments.")
	flag.IntVar(&options.D, "d", 10, "The number of data chunks for build-in redis client.")
	flag.IntVar(&options.P, "p", 2, "The number of parity chunks for build-in redis client.")
	// flag.BoolVar(&options.NoDashboard, "disable-dashboard", true, "Disable dashboard")
	showDashboard := flag.Bool("enable-dashboard", false, "Enable dashboard")
	flag.BoolVar(&options.NoColor, "disable-color", false, "Disable color log")
	flag.StringVar(&options.Pid, "pid", "/tmp/infinicache.pid", "Path to the pid.")
	flag.StringVar(&options.LogPath, "base", "", "Path to the log file.")
	flag.StringVar(&options.LogFile, "log", "", "File name of the log. If dashboard is not disabled, the default value is \"log\".")
	flag.BoolVar(&options.disableRecovery, "disable-recovery", false, "Disable data recovery on function reclaimation.")
	flag.StringVar(&options.cluster, "cluster", config.Cluster, "Cluster type. support \"static\" and \"window\"")
	flag.IntVar(&options.numFunctions, "functions", config.NumLambdaClusters, "Number of functions initialized at launch.")

	flag.BoolVar(&options.Evaluation, "enable-evaluation", false, "Enable evaluation settings.")
	flag.IntVar(&options.NumBackups, "numbak", 0, "EVALUATION ONLY: The number of backups used per node.")
	flag.BoolVar(&options.NoFirstD, "disable-first-d", false, "EVALUATION ONLY: Disable first-d optimization.")
	flag.Uint64Var(&options.funcCapacity, "funcap", 0, "EVALUATION ONLY: Preset capacity(MB) of function instance.")
	flag.StringVar(&options.invoker, "invoker", "lambda", "EVALUATION ONLY: Use alternative invokers. Try local")

	flag.StringVar(&options.CpuProfile, "cpuprofile", "", "Enable CPU profiling and write cpu profile to `file`")
	flag.StringVar(&options.MemProfile, "memprofile", "", "Enable memory profiling and write memory profile to `file`")

	flag.Parse()
	options.NoDashboard = !*showDashboard

	if printInfo {
		fmt.Fprintf(os.Stderr, "Usage: ./proxy [options]\n")
		fmt.Fprintf(os.Stderr, "Available options:\n")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if options.lambdaPrefix == "" {
		options.lambdaPrefix = config.LambdaPrefix
	} else {
		Log.Info("LambdaPrefix overrided: %s", options.lambdaPrefix)
	}

	if options.PublicIP != "" {
		ServerIp = options.PublicIP
	}
	Log.Info("Lambdas will connect to IP %s, make sure Lambdas are not deployed in the VPC if it is a public IP", ServerIp)

	if !options.NoDashboard {
		if options.LogFile == "" {
			options.LogFile = "log"
		}
		// options.NoColor = true
	}

	if options.disableRecovery {
		Flags |= protocol.FLAG_DISABLE_RECOVERY
	}

	if options.Evaluation && options.funcCapacity == 0 {
		fmt.Fprintf(os.Stderr, "Since evaluation is enabled, please specify the capacity of function instance with option \"-funcap\".\n")
		os.Exit(0)
	}

	if options.numFunctions > config.LambdaMaxDeployments {
		options.numFunctions = config.LambdaMaxDeployments
	} else if options.numFunctions < 1 {
		options.numFunctions = 1
	}
}
