// Copyright (c) 2021 James Lucas

package main

import "context"
import "errors"
import "fmt"
import "github.com/pborman/getopt/v2"
import log "github.com/sirupsen/logrus"
import "golang.org/x/term"
import "gopkg.in/yaml.v2"
import "os"
import "regexp"
import "sort"
import "strings"
import "sync"
import "time"

// Define custom log formatter with very minimal output.
// We're only really using log levels for verbosity - we
// don't need fancy formatting.
type PlainFormatter struct{}

func (f *PlainFormatter) Format(entry *log.Entry) ([]byte, error) {
	return []byte(entry.Message + "\n"), nil
}

type ConfigRule struct {
	Minrows  uint64             `yaml:"minrows"`
	Settings map[string]*string `yaml:"settings"`
}

type ConfigRuleset []ConfigRule

type ConfigMatchgroup struct {
	Schema  string `yaml:"schema"`
	Table   string `yaml:"table"`
	Owner   string `yaml:"owner"`
	Ruleset string `yaml:"ruleset"`
}

type ConfigFile struct {
	Matchgroups []ConfigMatchgroup       `yaml:"matchgroups"`
	Rulesets    map[string]ConfigRuleset `yaml:"rulesets"`
}

type TableMatchOption struct {
	OldSetting *string
	NewSetting *string
}

type TableMatch struct {
	Reloid         int
	Relkind        rune
	QuotedFullName string
	Owner          string
	Reltuples      int
	MatchgroupNum  int
	Matchgroup     *ConfigMatchgroup
	Options        map[string]TableMatchOption
}

func (tm *TableMatch) RelkindString() (string, error) {
	switch tm.Relkind {
	case 'r':
		return "Table", nil
	case 'm':
		return "Materialized View", nil
	default:
		return *new(string), errors.New(fmt.Sprintf("unrecognized relkind %c from database for %s", tm.Relkind, tm.QuotedFullName))
	}
}

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

	lastgroup := 0
	for _, val := range sortidx {
		if tms[val].MatchgroupNum != lastgroup {
			if lastgroup != 0 {
				log.Info("")
			}
			log.Infof(`Matchgroup %d (Ruleset: %s) - Schema: "%s", Table: "%s", Owner: "%s"`, tms[val].MatchgroupNum, tms[val].Matchgroup.Ruleset, tms[val].Matchgroup.Schema, tms[val].Matchgroup.Table, tms[val].Matchgroup.Owner)
			lastgroup = tms[val].MatchgroupNum
		}
		log.Infof(`  %-6s %-40s %-16s %11d rows`, objtype[tms[val].Relkind], tms[val].QuotedFullName, tms[val].Owner, tms[val].Reltuples)
	}
}

type ConnectOptions struct {
	Host     *string
	Port     *int
	Username *string
	Password *string
	DBName   *string
}

// builds a DSN from ConnectOptions
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

func (rslt *UpdateTableOptionsResult) OutputResult() {
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
		log.Warnf("%s %s [%d rows]:", objecttype, rslt.Match.QuotedFullName, rslt.Match.Reltuples)
	} else {
		log.Infof("%s %s [%d rows]:", objecttype, rslt.Match.QuotedFullName, rslt.Match.Reltuples)
	}
	for _, val := range rslt.SettingSuccess {
		if val.Success {
			if rslt.Match.Options[val.Setting].NewSetting == nil {
				log.Infof("  Reset %s (previous setting %s)", val.Setting, *rslt.Match.Options[val.Setting].OldSetting)
			} else {
				if rslt.Match.Options[val.Setting].OldSetting == nil {
					log.Infof("  Set %s to %s (previously unset)", val.Setting, *rslt.Match.Options[val.Setting].NewSetting)
				} else {
					log.Infof("  Set %s to %s (previous setting %s)", val.Setting, *rslt.Match.Options[val.Setting].NewSetting, *rslt.Match.Options[val.Setting].OldSetting)
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

// as soon as we encounter help flag, exit with usage
func GetoptCallback(opt getopt.Option) bool {
	if (opt.Name() == "-?" || opt.Name() == "--help") && opt.Seen() {
		usage(0)
	}
	return true
}

func main() {
	// set custom formatter for logging
	log.SetFormatter(new(PlainFormatter))
	log.SetLevel(log.WarnLevel)

	var connectoptions ConnectOptions

	opt_display_matches := getopt.BoolLong("display-matches", 0)
	opt_dry_run := getopt.BoolLong("dry-run", 'n')
	opt_jobs := getopt.IntLong("jobs", 'j', 1)
	opt_lock_timeout := new(float64)
	getopt.FlagLong(opt_lock_timeout, "lock-timeout", 0)
	opt_skip_locked := getopt.BoolLong("skip-locked", 0)
	opt_verbose := getopt.BoolLong("verbose", 'v')
	// handled by callback, we don't store the value
	getopt.BoolLong("help", '?')
	connectoptions.Host = getopt.StringLong("host", 'h', "")
	connectoptions.Port = getopt.IntLong("port", 'p', -1)
	connectoptions.Username = getopt.StringLong("username", 'U', "")
	opt_no_password := getopt.BoolLong("no-password", 'w')
	opt_password := getopt.BoolLong("password", 'W')
	connectoptions.DBName = getopt.StringLong("dbname", 'd', "")

	err := getopt.Getopt(GetoptCallback)
	if err != nil {
		log.Fatal(err)
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
		log.SetLevel(log.InfoLevel)
	}

	x := ConfigFile{}

	// read the config file
	dat, err := os.ReadFile("test.yml")
	if err != nil {
		log.Fatal(err)
	}

	// parse it
	err = yaml.UnmarshalStrict(dat, &x)
	if err != nil {
		log.Fatal(err)
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

	// retrieve all the matching tables
	tablematches, err := conn.GetTableMatches(x.Matchgroups, x.Rulesets)
	if err != nil {
		log.Fatal(err)
	}

	// in display-matches mode, we output the matches here and then exit
	if *opt_display_matches {
		log.SetLevel(log.InfoLevel)
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
				rslt, err := conn.UpdateTableOptions(m, *opt_dry_run, WaitModeNowait, 0)
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
				rslt, err := conn.UpdateTableOptions(m, false, WaitModeWait, *opt_lock_timeout)
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

	os.Exit(0)
}
