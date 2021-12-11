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
	Database string `yaml:"database"`
	Schema   string `yaml:"schema"`
	Table    string `yaml:"table"`
	Ruleset  string `yaml:"ruleset"`
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
  -v, --verbose                   write a lot of output
  -?, --help                      show this help, then exit

Connection Options:
  -h, --host=HOSTNAME       database server host or socket directory
  -p, --port=PORT           database server port
  -U, --username=USERNAME   user name to connect as
  -w, --no-password         never prompt for password
  -W, --password            force password prompt
  -d, --dbname              initial database name to connect to (default: "postgres")

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

	x := configFileType{}

	_ = opt_verbose
	_ = opt_help
	_ = opt_no_password
	_ = opt_password

	// read the config file
	dat, err := os.ReadFile("test.yml")
	if err != nil {
		panic(err)
	}

	// parse it
	err = yaml.Unmarshal(dat, &x)
	if err != nil {
		panic(err)
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
	initialconn, err := NewDBInterface(&connectoptions)
	if err != nil {
		var pwerr *PasswordAuthenticationError
		if errors.As(err, &pwerr) && !(*opt_password || *opt_no_password) {
			err := connectoptions.PromptPassword()
			if err != nil {
				log.Fatal(err)
			}
			initialconn, err = NewDBInterface(&connectoptions)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	matcheddbnames, initialdbmatch, err := initialconn.GetDBMatches(x.Match)
	if err != nil {
		initialconn.Close()
		log.Fatal(err)
	}
	log.Debug(matcheddbnames)
	// close the initial connection unless we can reuse it
	if !initialdbmatch {
		initialconn.Close()
	}

	for idx, val := range matcheddbnames {
		var currconn *DBInterface
		if idx == 0 && initialdbmatch {
			currconn = initialconn
			log.Infof("Reusing connection to %s", val)
		} else {
			perdbconfig := connectoptions
			perdbconfig.DBName = &val
			currconn, err = NewDBInterface(&connectoptions)
			if err != nil {
				log.Fatal(err)
			}
			log.Infof("Connected to %s", val)
		}
		tablematches, err := currconn.GetTableMatches(val, x.Match, x.Ruleset)
		if err != nil {
			panic(err)
		}

		for _, val := range tablematches {
			log.Infof("Table %s:", val.QuotedFullName)
			//err := currconn.UpdateTableOptions(val, false)
			err := currconn.UpdateTableOptions(val, false, WaitModeWait, 0)
			if err != nil {
				var alerr *AcquireLockError
				if errors.As(err, &alerr) {
					log.Warnf("%v", err)
				} else {
					panic(err)
				}
			}
		}

		currconn.Close()
	}
}
