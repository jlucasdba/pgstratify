// Copyright (c) 2021 James Lucas

package main

//import "context"
//import "github.com/jackc/pgx/v4"
import "errors"
import "fmt"
import "github.com/pborman/getopt/v2"
import log "github.com/sirupsen/logrus"
import "golang.org/x/term"
import "gopkg.in/yaml.v2"
import "os"
import "regexp"
import "strings"

// Define custom log formatter with very minimal output.
// We're only really using log levels for verbosity - we
// don't need fancy formatting.
type PlainFormatter struct{}

func (f *PlainFormatter) Format(entry *log.Entry) ([]byte, error) {
	return []byte(entry.Message + "\n"), nil
}

type ruleType struct {
	Condition string            `yaml:"condition"`
	Value     uint64            `yaml:"value"`
	Set       map[string]string `yaml:"set"`
	Reset     []string          `yaml:"reset"`
}

type rulesetType map[string][]ruleType

type matchType struct {
	Schema  string `yaml:"schema"`
	Table   string `yaml:"table"`
	Ruleset string `yaml:"ruleset"`
}

type configFileType struct {
	Ruleset rulesetType
	Match   []matchType
}

type tableMatchOption struct {
	OldSetting *string
	NewSetting *string
}

type tableMatch struct {
	Reloid         int
	QuotedFullName string
	Options        map[string]tableMatchOption
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

// display usage message, then exit with status
func usage(status int) {
	fmt.Printf(`pgvacman scans the database and modifies storage parameters based on rules.

Usage:
  %s [OPTION] ... [RULEFILE]

Options:
  -j, --jobs=NUM                  use this many concurrent connections to set storage parameters
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
	log.SetLevel(log.InfoLevel)

	var connectoptions ConnectOptions

	opt_jobs := getopt.IntLong("jobs", 'j', 1)
	opt_verbose := getopt.BoolLong("verbose", 'v')
	opt_help := getopt.BoolLong("help", '?')
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

	x := configFileType{}

	_ = opt_verbose
	_ = opt_help
	_ = opt_no_password
	_ = opt_password

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
	tablematches, err := conn.GetTableMatches(x.Match, x.Ruleset)
	if err != nil {
		log.Fatal(err)
	}

	// allocate db connections up to *opt_jobs (or len(tablematches), whichever is less)
	connections := []*DBInterface{conn}
	for i := 1; i < func(a int, b int) int {
		if a < b {
			return a
		}
		return b
	}(len(tablematches), *opt_jobs); i++ {
		newconn, err := NewDBInterface(&connectoptions)
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
	matchiter := make(chan tableMatch)
	go func(matchiter chan<- tableMatch) {
		for _, v := range tablematches {
			matchiter <- v
		}
		close(matchiter)
	}(matchiter)

	// goroutine receiving failed tablematches from workers
	lockpendingrcv := make(chan tableMatch)
	lockpendingret := make(chan []tableMatch)
	go func(matchin <-chan tableMatch, matchesout chan<- []tableMatch) {
		lockpending := make([]tableMatch, 0)
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
	donechans := make([]chan bool, 0, len(connections))
	for _, val := range connections {
		donechan := make(chan bool)
		donechans = append(donechans, donechan)
		go func(conn *DBInterface, lockpendingrcv chan<- tableMatch, donechan chan<- bool) {
			for m := range matchiter {
				err := conn.UpdateTableOptions(m, false, WaitModeNowait, 0)
				if err != nil {
					var alerr *AcquireLockError
					if errors.As(err, &alerr) {
						lockpendingrcv <- m
					} else {
						log.Fatal(err)
					}
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
			val.Close()
		}
		os.Exit(1)
	}

	// otherwise, if we have more connections than pending tables, close some
	overconns := len(connections) - len(lockpending)
	if overconns > 0 {
		for _, val := range connections[len(connections)-overconns : len(connections)] {
			val.Close()
		}
		connections = append([]*DBInterface(nil), connections[0:len(connections)-overconns]...)
	}

	// now another iterator goroutine to cycle through the remaining tables
	matchiter = make(chan tableMatch)
	go func(matchiter chan<- tableMatch) {
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
				err := conn.UpdateTableOptions(m, false, WaitModeWait, -1)
				if err != nil {
					var alerr *AcquireLockError
					if errors.As(err, &alerr) {
						log.Warn(err)
					} else {
						log.Fatal(err)
					}
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
