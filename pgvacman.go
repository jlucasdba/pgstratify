package main

//import "context"
//import "github.com/jackc/pgx/v4"
import "errors"
import "fmt"
import log "github.com/sirupsen/logrus"
import "gopkg.in/yaml.v2"
import "os"
import "strings"

// Define custom log formatter with very minimal output.
// We're only really using log levels for verbosity - we
// don't need fancy formatting.
type PlainFormatter struct{}

func (f *PlainFormatter) Format(entry *log.Entry) ([]byte, error) {
	return []byte(entry.Message + "\n"), nil
}

type configSectionType map[string]string

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
	Config  configSectionType
	Ruleset rulesetType
	Match   []matchType
}

type table struct {
	SchemaName     string
	TableName      string
	QuotedFullName string
}

type databaseMatches map[string]bool

func newDatabaseMatches() map[string]bool {
	return make(map[string]bool, 0)
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

func buildDSN(conf configSectionType) string {
	components := make([]string, 0)
	for k, v := range conf {
		if v != "" {
			components = append(components, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return strings.Join(components, " ")
}

func main() {
	// set custom formatter for logging
	log.SetFormatter(new(PlainFormatter))
	log.SetLevel(log.InfoLevel)

	x := configFileType{}

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

	dsn := buildDSN(x.Config)
	log.Debug(dsn)
	log.Debug(buildURL(x.Config))
	// connect to the database
	/*
		conn,err := pgx.Connect(context.Background(),dsn)
		if err != nil {
			panic(err)
		}
		rows,err := conn.Query(context.Background(),"select datname from pg_database where datallowconn = 't'")
		if err != nil {
			panic(err)
		}
		{
			var n string
			defer rows.Close()
			for rows.Next() {
				rows.Scan(&n)
				fmt.Println(n)
			}
		}
		defer conn.Close(context.Background())
	*/
	initialconn := NewDBInterface(buildDSN(x.Config))
	/*
		dbs := i.ListDBs()
		for _, val := range dbs {
			fmt.Println(val)
		}
	*/
	matcheddbnames, initialdbmatch, err := initialconn.GetDBMatches(x.Match)
	if err != nil {
		initialconn.Close()
		panic(err)
	}
	log.Debug(matcheddbnames)
	// close the initial connection unless we can reuse it
	if !initialdbmatch {
		initialconn.Close()
	}

	for idx, val := range matcheddbnames {
		var currconn DBInterface
		if idx == 0 && initialdbmatch {
			currconn = initialconn
			log.Infof("Reusing connection to %s", val)
		} else {
			perdbconfig := x.Config
			perdbconfig["dbname"] = val
			currconn = NewDBInterface(buildDSN(perdbconfig))
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
