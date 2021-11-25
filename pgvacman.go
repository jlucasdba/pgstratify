package main

//import "context"
//import "github.com/jackc/pgx/v4"
import "fmt"
import "gopkg.in/yaml.v2"
import "os"
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
	i := NewDBInterface(buildDSN(x.Config))
	defer i.Close()
	dbs := i.ListDBs()
	for _, val := range dbs {
		fmt.Println(val)
	}
}
