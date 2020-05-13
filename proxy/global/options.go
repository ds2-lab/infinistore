package global

type CommandlineOptions struct {
	Pid           string
	Debug         bool
	Prefix        string
	D             int
	P             int
	NoDashboard   bool
	NoColor       bool
	LogPath       string
	LogFile       string
	Evaluation    bool
	NumBackups    int
	NoFirstD      bool
}
