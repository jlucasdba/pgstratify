// Copyright (c) 2022 James Lucas

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/pborman/getopt/v2"

	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/term"
	"gopkg.in/yaml.v2"
)

const Version string = "0.0.1"

// Define custom log formatter with very minimal output.
// We're only really using log levels for verbosity - we
// don't need fancy formatting.
type PlainFormatter struct{}

func (f *PlainFormatter) Format(entry *log.Entry) ([]byte, error) {
	return []byte(entry.Message + "\n"), nil
}

// custom logging hook to set output to stdout for info, debug, and trace
type OutHook struct{}

func (oh *OutHook) Levels() []log.Level {
	return []log.Level{log.InfoLevel, log.DebugLevel, log.TraceLevel}
}

func (oh *OutHook) Fire(e *log.Entry) error {
	log.SetOutput(os.Stdout)
	return nil
}

// custom logging hook to set output to stderr for panic, fatal, error, and warn
type ErrHook struct{}

func (eh *ErrHook) Levels() []log.Level {
	return []log.Level{log.PanicLevel, log.FatalLevel, log.ErrorLevel, log.WarnLevel}
}

func (eh *ErrHook) Fire(e *log.Entry) error {
	log.SetOutput(os.Stderr)
	return nil
}

// individual rule definition from yaml config
type ConfigRule struct {
	Minrows  uint64             `yaml:"minrows"`
	Settings map[string]*string `yaml:"settings"`
}

// set of related rules
type ConfigRuleset []ConfigRule

// unmarshaling of ruleset with some additional validation (no duplicate minrows)
func (cr *ConfigRuleset) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// can't go direct to ConfigRuleset because it will call this method again,
	// recursing forever
	var r []ConfigRule
	err := unmarshal(&r)
	if err != nil {
		return err
	}

	if r == nil {
		cr = nil
	}

	m := make(map[uint64]bool)
	for _, val := range r {
		if m[val.Minrows] {
			return fmt.Errorf("duplicate value `%d` found in ruleset", val.Minrows)
		}
		m[val.Minrows] = true
	}

	*cr = ConfigRuleset(r)
	return err
}

// matchgroup from yaml config
type ConfigMatchgroup struct {
	Schema        string `yaml:"schema"`
	Table         string `yaml:"table"`
	Owner         string `yaml:"owner"`
	CaseSensitive bool   `yaml:"case_sensitive"`
	Ruleset       string `yaml:"ruleset"`
}

// overall yaml config file
type ConfigFile struct {
	Matchgroups []ConfigMatchgroup       `yaml:"matchgroups"`
	Rulesets    map[string]ConfigRuleset `yaml:"rulesets"`
}

// old and new settings for a table parameter
type TableMatchParameter struct {
	OldSetting *string
	NewSetting *string
}

// table that matched in the database, with parameters in need of update
type TableMatch struct {
	Reloid         int
	Relkind        rune
	QuotedFullName string
	Owner          string
	Reltuples      int
	MatchgroupNum  int
	Matchgroup     *ConfigMatchgroup
	Parameters     map[string]TableMatchParameter
}

// returns correct sql type specifier for this tablematch
func (tm *TableMatch) RelkindString() (string, error) {
	switch tm.Relkind {
	case 'r':
		return "Table", nil
	case 'm':
		return "Materialized View", nil
	default:
		return *new(string), fmt.Errorf("unrecognized relkind %c from database for %s", tm.Relkind, tm.QuotedFullName)
	}
}

// given a slice of TableMatches, display them on the console for configuration debugging
func MatchDisplay(tms []TableMatch) {
	sortidx := make([]int, len(tms))
	for i := 0; i < len(sortidx); i++ {
		sortidx[i] = i
	}

	// Not totally happy with this sorting logic, but it works
	sort.SliceStable(sortidx, func(i, j int) bool {
		if tms[i].MatchgroupNum == tms[j].MatchgroupNum {
			if tms[i].Reltuples == tms[j].Reltuples {
				return tms[i].QuotedFullName < tms[j].QuotedFullName
			}
			return tms[i].Reltuples > tms[j].Reltuples
		}
		return tms[i].MatchgroupNum < tms[j].MatchgroupNum
	})

	objtype := map[rune]string{'r': "TABLE", 'm': "MVIEW"}
	csmap := map[bool]rune{true: 't', false: 'f'}

	lastgroup := 0
	for _, val := range sortidx {
		if tms[val].MatchgroupNum != lastgroup {
			if lastgroup != 0 {
				log.Debug("")
			}
			log.Debugf(`Matchgroup %d (Ruleset: %s) - Schema: "%s", Table: "%s", Owner: "%s", CaseSensitive: %c`, tms[val].MatchgroupNum, tms[val].Matchgroup.Ruleset, tms[val].Matchgroup.Schema, tms[val].Matchgroup.Table, tms[val].Matchgroup.Owner, csmap[tms[val].Matchgroup.CaseSensitive])
			lastgroup = tms[val].MatchgroupNum
		}
		log.Debugf(`  %-6s %-40s %-16s %11d rows`, objtype[tms[val].Relkind], tms[val].QuotedFullName, tms[val].Owner, tms[val].Reltuples)
	}
}

// database connection options
type ConnectOptions struct {
	Host     *string
	Port     *int
	Username *string
	Password *string
	DBName   *string
}

// build a DSN from ConnectOptions
func (co *ConnectOptions) BuildDSN() string {
	components := make([]string, 0)
	escaped := co.EscapeStrings()
	if escaped.Host != nil && *escaped.Host != "" {
		components = append(components, fmt.Sprintf("host='%s'", *escaped.Host))
	}
	if escaped.Port != nil && *escaped.Port >= 0 {
		components = append(components, fmt.Sprintf("port=%d", *escaped.Port))
	}
	if escaped.Username != nil && *escaped.Username != "" {
		components = append(components, fmt.Sprintf("user='%s'", *escaped.Username))
	}
	if escaped.Password != nil && *escaped.Password != "" {
		components = append(components, fmt.Sprintf("password='%s'", *escaped.Password))
	}
	if escaped.DBName != nil && *escaped.DBName != "" {
		components = append(components, fmt.Sprintf("dbname='%s'", *escaped.DBName))
	}
	return strings.Join(components, " ")
}

// returns a copy of ConnectOptions with member strings escaped
func (co ConnectOptions) EscapeStrings() ConnectOptions {
	replacere, err := regexp.Compile(`(['\\])`)
	if err != nil {
		log.Panic(err)
	}
	if co.Host != nil {
		host := replacere.ReplaceAllString(*co.Host, `\$1`)
		co.Host = &host
	}
	if co.Username != nil {
		user := replacere.ReplaceAllString(*co.Username, `\$1`)
		co.Username = &user
	}
	if co.Password != nil {
		password := replacere.ReplaceAllString(*co.Password, `\$1`)
		co.Password = &password
	}
	if co.DBName != nil {
		dbname := replacere.ReplaceAllString(*co.DBName, `\$1`)
		co.DBName = &dbname
	}

	return co
}

// prompt for password
func (co *ConnectOptions) PromptPassword() error {
	for {
		fmt.Print("Password: ")
		buf, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println("")
		if err != nil {
			return err
		}
		if cap(buf) > 0 {
			str := string(buf)
			co.Password = &str
			return nil
		}
	}
}

// runtime statistics for output at end of run
type RunStats struct {
	TablesMatched       int
	MViewsMatched       int
	ParametersMatched   int
	ParametersAttempted int
	ParametersSet       int
	ParametersErrored   int
	accessLock          sync.Mutex
}

// update the paramter stats - this method will be accessed from goroutines so it needs a mutex
func (rs *RunStats) UpdateFromResult(result *UpdateTableParametersResult) {
	rs.accessLock.Lock()
	defer rs.accessLock.Unlock()
	for _, val := range result.SettingSuccess {
		rs.ParametersAttempted++
		if val.Success {
			rs.ParametersSet++
		} else {
			rs.ParametersErrored++
		}
	}
}

// output the runtime stats
func (rs *RunStats) OutputStats() {
	log.Infof("%d Objects Matched, %d Parameters Modified, %d Parameter Errors", rs.TablesMatched+rs.MViewsMatched, rs.ParametersSet, rs.ParametersErrored)
}

// output the runtime stats for a dry-run (different formatting)
func (rs *RunStats) OutputStatsDryRun() {
	log.Infof("%d Objects Matched, %d Parameters Modified (Dry-Run)", rs.TablesMatched+rs.MViewsMatched, rs.ParametersSet)
}

// this is here instead of dbinterface file because it's user-facing output
func (rslt *UpdateTableParametersResult) OutputResult() {
	anyfailed := false
	for _, val := range rslt.SettingSuccess {
		if !val.Success {
			anyfailed = true
		}
	}

	objecttype, err := rslt.Match.RelkindString()
	if err != nil {
		log.Fatal(err)
	}

	if anyfailed {
		log.Infof("%s %s [%d rows]:", objecttype, rslt.Match.QuotedFullName, rslt.Match.Reltuples)
	} else {
		log.Debugf("%s %s [%d rows]:", objecttype, rslt.Match.QuotedFullName, rslt.Match.Reltuples)
	}
	for _, val := range rslt.SettingSuccess {
		if val.Success {
			if rslt.Match.Parameters[val.Setting].NewSetting == nil {
				log.Debugf("  Reset %s (previous setting %s)", val.Setting, *rslt.Match.Parameters[val.Setting].OldSetting)
			} else {
				if rslt.Match.Parameters[val.Setting].OldSetting == nil {
					log.Debugf("  Set %s to %s (previously unset)", val.Setting, *rslt.Match.Parameters[val.Setting].NewSetting)
				} else {
					log.Debugf("  Set %s to %s (previous setting %s)", val.Setting, *rslt.Match.Parameters[val.Setting].NewSetting, *rslt.Match.Parameters[val.Setting].OldSetting)
				}
			}
		} else {
			log.Warnf("  Failed to set %s: %v", val.Setting, val.Err)
		}
	}
}

// display usage message, then exit with status
func usage(status int) {
	fmt.Printf(`pgvacman scans the database and modifies storage parameters based on rules.

Usage:
  %s [OPTION] ... [RULEFILE]

Options:
      --display-matches           take no action, and display what tables matched each matchgroup
  -n, --dry-run                   output what would be done without making changes (implies -v)
  -j, --jobs=NUM                  use this many concurrent connections to set storage parameters
      --lock-timeout=NUM          per-table wait timeout in seconds (must be greater than 0, no effect in skip-locked mode)
      --skip-locked               skip tables that cannot be immediately locked
  -v, --verbose                   write a lot of output
  -V, --version                   output version information, then exit
  -?, --help                      show this help, then exit

Connection Options:
  -h, --host=HOSTNAME       database server host or socket directory
  -p, --port=PORT           database server port
  -U, --username=USERNAME   user name to connect as
  -w, --no-password         never prompt for password
  -W, --password            force password prompt
  -d, --dbname              database name to connect to and update

`, os.Args[0])

	os.Exit(status)
}

func main() {
	// set custom formatter for logging
	log.SetFormatter(new(PlainFormatter))
	// set custom hooks
	log.AddHook(new(OutHook))
	log.AddHook(new(ErrHook))
	// default to Info level
	log.SetLevel(log.InfoLevel)

	var connectoptions ConnectOptions

	opt_display_matches := getopt.BoolLong("display-matches", 0)
	opt_dry_run := getopt.BoolLong("dry-run", 'n')
	opt_jobs := getopt.IntLong("jobs", 'j', 1)
	opt_lock_timeout := new(float64)
	getopt.FlagLong(opt_lock_timeout, "lock-timeout", 0)
	opt_skip_locked := getopt.BoolLong("skip-locked", 0)
	opt_verbose := getopt.BoolLong("verbose", 'v')
	opt_version := getopt.BoolLong("version", 'V')
	opt_help := getopt.BoolLong("help", '?')
	connectoptions.Host = getopt.StringLong("host", 'h', "")
	connectoptions.Port = getopt.IntLong("port", 'p', -1)
	connectoptions.Username = getopt.StringLong("username", 'U', "")
	opt_no_password := getopt.BoolLong("no-password", 'w')
	opt_password := getopt.BoolLong("password", 'W')
	connectoptions.DBName = getopt.StringLong("dbname", 'd', "")

	// as soon as we encounter help flag, exit with usage
	// we check this first, so help takes priority over any other options
	for _, val := range os.Args {
		if val == "-?" || val == "--help" {
			usage(0)
		} else if val == "--" {
			break
		}
	}

	err := getopt.Getopt(nil)
	if err != nil {
		log.Fatal(err)
	}

	// shouldn't get here, but doesn't hurt to check
	if *opt_help {
		usage(0)
	}

	if *opt_version {
		log.Infof("pgvacman %s", Version)
		os.Exit(0)
	}

	if *opt_jobs < 1 {
		log.Fatal(errors.New("number of parallel jobs must be at least 1"))
	}

	if getopt.GetCount("lock-timeout") == 0 {
		*opt_lock_timeout = -1
	} else if *opt_lock_timeout <= 0 {
		log.Fatal(errors.New("lock-timeout, when specified, must be greater than 0"))
	}

	// dry-run implies verbose
	if *opt_dry_run {
		*opt_verbose = true
	}

	if *opt_verbose {
		log.SetLevel(log.DebugLevel)
	}

	x := ConfigFile{}

	// read the config file
	if len(getopt.Args()) < 1 {
		log.Fatal(fmt.Errorf("rulefile name must be specified"))
	} else if len(getopt.Args()) > 1 {
		log.Fatal(fmt.Errorf("more than one rulefile name may not be specified"))
	}
	dat, err := os.ReadFile(getopt.Args()[0])
	if err != nil {
		log.Fatal(err)
	}

	// parse it
	err = yaml.UnmarshalStrict(dat, &x)
	if err != nil {
		/*
			yaml.TypeError's string representation exposes implementation details,
			like type names, so we perform string substitution to hide that.
		*/
		x := new(yaml.TypeError)
		if errors.As(err, &x) {
			intypere, reerr := regexp.Compile(`(?m) in type .*$`)
			if reerr != nil {
				log.Panic(reerr)
			}
			intore, reerr := regexp.Compile(`(?m) cannot unmarshal !!.+ ` + "`" + `(.*)` + "`" + ` .*$`)
			if reerr != nil {
				log.Panic(reerr)
			}

			if intore.MatchString(x.Error()) {
				errstr := intore.ReplaceAllString(x.Error(), " invalid value `$1`")
				log.Fatal(errstr)
			}

			errstr := intypere.ReplaceAllLiteralString(x.Error(), "")
			log.Fatal(errstr)
		} else {
			log.Fatal(err)
		}
	}

	// connect to the database
	// if -W was passed, prompt for password up front
	if *opt_password {
		err := connectoptions.PromptPassword()
		if err != nil {
			log.Fatal(err)
		}
	}
	// otherwise, we attempt to connect
	// if initial attempt fails, -w was not passed, and
	// we haven't previously prompted, prompt for password
	// and try again
	conn, err := NewDBInterface(&connectoptions)
	if err != nil {
		var pwerr *PasswordAuthenticationError
		if errors.As(err, &pwerr) && !(*opt_password || *opt_no_password) {
			err := connectoptions.PromptPassword()
			if err != nil {
				log.Fatal(err)
			}
			conn, err = NewDBInterface(&connectoptions)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}

	log.Infof(`pgvacman: updating storage parameters for database "%s"`, conn.CurrentDB())

	// retrieve all the matching tables
	tablematches, err := conn.GetTableMatches(x.Matchgroups, x.Rulesets)
	if err != nil {
		log.Fatal(err)
	}

	// populate run stats
	var runstats RunStats
	for _, val := range tablematches {
		switch val.Relkind {
		case 'r':
			runstats.TablesMatched++
		case 'm':
			runstats.MViewsMatched++
		}
		for range val.Parameters {
			runstats.ParametersMatched++
		}
	}

	// in display-matches mode, we output the matches here and then exit
	if *opt_display_matches {
		log.SetLevel(log.DebugLevel)
		MatchDisplay(tablematches)
		os.Exit(0)
	}

	// allocate db connections up to *opt_jobs (or len(tablematches), whichever is less)
	connections := []*DBInterface{conn}
	for i := 1; i < func(a int, b int) int {
		if a < b {
			return a
		}
		return b
	}(len(tablematches), *opt_jobs); i++ {
		var newconn *DBInterface
		var err error
		if *opt_dry_run {
			/*
				An ugly hack, but in the case of a dry-run, there's
				no need to open additional connections to the database,
				but we still want the structs so we can use their
				methods without having totally separate execution flow.
				Zero structs should be enough for this limited case.
			*/
			newconn, err = new(DBInterface), nil
		} else {
			newconn, err = NewDBInterface(&connectoptions)
		}
		if err != nil {
			log.Fatal(err)
		}
		connections = append(connections, newconn)
	}

	/*
		We make a first opportunistic pass through the tables and try to set
		parameters in nowait mode. Hopefully this knocks out the majority of
		the tables near the start of the run.
		We suppress output of anything that failed to lock during this pass
		because they will be retried.
	*/

	// goroutine iterating over tablematches and returning them on a channel
	matchiter := make(chan TableMatch)
	go func(matchiter chan<- TableMatch) {
		for _, v := range tablematches {
			matchiter <- v
		}
		close(matchiter)
	}(matchiter)

	// goroutine receiving failed tablematches from workers
	lockpendingrcv := make(chan TableMatch)
	lockpendingret := make(chan []TableMatch)
	go func(matchin <-chan TableMatch, matchesout chan<- []TableMatch) {
		lockpending := make([]TableMatch, 0)
		for m := range matchin {
			lockpending = append(lockpending, m)
		}
		matchesout <- lockpending
	}(lockpendingrcv, lockpendingret)

	/*
		Launch a goroutine for each connection, each reading matches from matchiter.
		Tables that fail to lock are fed to lockpendingrcv (other errors are fatal).
		When matchiter is closed, close donechan to signal goroutine is complete.
	*/
	// mutex for synchronizing multi-line output - it's not worth juggling more channels for this
	// log is already threadsafe - this is just to keep goroutines from interleaving output lines
	var outmutex sync.Mutex
	donechans := make([]chan bool, 0, len(connections))
	for _, val := range connections {
		donechan := make(chan bool)
		donechans = append(donechans, donechan)
		go func(conn *DBInterface, lockpendingrcv chan<- TableMatch, donechan chan<- bool) {
			for m := range matchiter {
				rslt, err := conn.UpdateTableParameters(m, *opt_dry_run, WaitModeNowait, 0)
				if err != nil {
					var alerr *AcquireLockError
					if errors.As(err, &alerr) {
						if *opt_skip_locked {
							outmutex.Lock()
							// in skip-locked modes, don't emit to channel
							// also we need to output even on lock failure
							rslt.OutputResult()
							// we also want to emit the warning in skip-locked mode
							log.Warn(err)
							outmutex.Unlock()
						} else {
							lockpendingrcv <- m
						}
					} else {
						log.Fatal(err)
					}
				} else {
					// only output on sucess since tables will be retried
					outmutex.Lock()
					rslt.OutputResult()
					outmutex.Unlock()
				}
				// record result stats - mutex synchronized internally
				runstats.UpdateFromResult(&rslt)
			}
			close(donechan)
		}(val, lockpendingrcv, donechan)
	}

	// wait until all donechans are closed
	for _, donechan := range donechans {
		<-donechan
	}

	// close lockpendingrcv
	close(lockpendingrcv)

	// retrieve lockpending, and close lockpendingret
	lockpending := <-lockpendingret
	close(lockpendingret)

	// if nothing is pending, we are done
	if len(lockpending) == 0 {
		// close all connections
		for _, val := range connections {
			if *opt_dry_run && val.conn == nil {
			} else {
				val.Close()
			}
		}
		if *opt_dry_run {
			runstats.OutputStatsDryRun()
		} else {
			runstats.OutputStats()
		}
		os.Exit(0)
	}

	// otherwise, if we have more connections than pending tables, close some
	overconns := len(connections) - len(lockpending)
	if overconns > 0 {
		for _, val := range connections[len(connections)-overconns:] {
			val.Close()
		}
		connections = append([]*DBInterface(nil), connections[0:len(connections)-overconns]...)
	}

	// now another iterator goroutine to cycle through the remaining tables
	matchiter = make(chan TableMatch)
	go func(matchiter chan<- TableMatch) {
		for _, v := range lockpending {
			matchiter <- v
		}
		close(matchiter)
	}(matchiter)

	// goroutines for each connection, pulling from matchiter and modifying in wait mode
	donechans = make([]chan bool, 0, len(connections))
	for _, val := range connections {
		donechan := make(chan bool)
		donechans = append(donechans, donechan)
		go func(conn *DBInterface, donechan chan<- bool) {
			for m := range matchiter {
				// if we wait more than a second, output a wait message
				waitctx, waitcancel := context.WithCancel(context.Background())
				go func() {
					timer := time.NewTimer(time.Second)
					select {
					case <-waitctx.Done():
						break
					case <-timer.C:
						log.Warnf("Waiting for lock on table %s", m.QuotedFullName)
					}
					// drain the channel, per the docs
					if !timer.Stop() {
						<-timer.C
					}
				}()
				rslt, err := conn.UpdateTableParameters(m, false, WaitModeWait, *opt_lock_timeout)
				// cancel the wait - if the message fired already this does nothing
				waitcancel()
				if err != nil {
					var alerr *AcquireLockError
					if errors.As(err, &alerr) {
						log.Warn(err)
					} else {
						log.Fatal(err)
					}
				} else {
					outmutex.Lock()
					rslt.OutputResult()
					outmutex.Unlock()
				}
				// record result stats - mutex synchronized internally
				runstats.UpdateFromResult(&rslt)
			}
			close(donechan)
			// close the connection when we're done as well
			conn.Close()
		}(val, donechan)
	}

	// wait until all donechans are closed
	for _, donechan := range donechans {
		<-donechan
	}

	runstats.OutputStats()
	os.Exit(0)
}
