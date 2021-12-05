package main

//import "context"
//import "github.com/jackc/pgx/v4"
import "fmt"
import "gopkg.in/yaml.v2"
import "os"
import "sort"
import "strings"

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
	fmt.Println(dsn)
	fmt.Println(buildURL(x.Config))
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
	fmt.Println(matcheddbnames)
	// close the initial connection unless we can reuse it
	if !initialdbmatch {
		initialconn.Close()
	}

	for idx, val := range matcheddbnames {
		var currconn DBInterface
		if idx == 0 && initialdbmatch {
			currconn = initialconn
			fmt.Printf("Reusing connection to %s\n", val)
		} else {
			perdbconfig := x.Config
			perdbconfig["dbname"] = val
			currconn = NewDBInterface(buildDSN(perdbconfig))
			fmt.Printf("Connected to %s\n", val)
		}
		tablematches, err := currconn.GetTableMatches(val, x.Match, x.Ruleset)
		if err != nil {
			panic(err)
		}

		for _, val := range tablematches {
			fmt.Printf("Table %s:\n", val.QuotedFullName)
			sortedkeys := make([]string, 0, len(val.Options))
			for key2, _ := range val.Options {
				sortedkeys = append(sortedkeys, key2)
			}
			sort.Strings(sortedkeys)
			for _, val2 := range sortedkeys {
				if val.Options[val2].NewSetting == nil {
					fmt.Printf("  Reset %s\n", val2)
				} else if val.Options[val2].OldSetting == nil {
					fmt.Printf("  Set %s to %s (previously unset)\n", val2, *val.Options[val2].NewSetting)
				} else {
					fmt.Printf("  Set %s to %s (previous setting %s)\n", val2, *val.Options[val2].NewSetting, *val.Options[val2].OldSetting)
				}
			}
		}

		currconn.Close()
	}
}
