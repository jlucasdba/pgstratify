package main

import "context"
import "encoding/json"
import "github.com/jackc/pgx/v4"

var bgctx = context.Background()

// Struct wrapping a database connection.
type DBInterface struct {
	dsn  string
	conn *pgx.Conn
}

func NewDBInterface(dsn string) DBInterface {
	i := DBInterface{}
	i.dsn = dsn
	conn, err := pgx.Connect(bgctx, dsn)
	if err != nil {
		panic(err)
	}
	i.conn = conn
	return i
}

func (i *DBInterface) Close() {
	i.conn.Close(bgctx)
}

func (i *DBInterface) ListDBs() []string {
	datnames := make([]string, 0)
	r, err := i.conn.Query(bgctx, "select datname from pg_database where datallowconn = 't'")
	if err != nil {
		panic(err)
	}
	for r.Next() {
		var s string
		err := r.Scan(&s)
		if err != nil {
			r.Close()
			panic(err)
		}
		datnames = append(datnames, s)
	}
	r.Close()
	return datnames
}

/*
Takes a slice of matchType representing the database match rules from the config file in order.
Returns four items: First, a slice of databaseMatches (map of database names), with each element
corresponding to one of the match rules from the input slice. Second, an aggregated list of matched
database names, with the initial connection database first (if applicable) and otherwise sorted
descending by database size. Last, a bool representing whether the initial database connection
was found in the matches. And lastly an error.
*/
func (i *DBInterface) GetDBMatches(matchconfig []matchType) ([]databaseMatches, []string, bool, error) {
	matchdbre := make([]string, 0, 10)
	dbmatches := make([]databaseMatches, 0)
	matcheddbmap := make(map[string]bool)
	matcheddblist := make([]string, 0)
	initialmatch := false

	for _, v := range matchconfig {
		matchdbre = append(matchdbre, v.Database)
		dbmatches = append(dbmatches, newDatabaseMatches())
	}
	r, err := i.conn.Query(bgctx, "select x.rownum, d.datname, case when d.datname=current_database() then 't'::bool else 'f'::bool end as initial from pg_database d join (select row_number() over () as rownum, re from (select unnest($1::text[]) as re) xx) x on d.datname ~ x.re where datallowconn='t' order by initial desc, pg_database_size(datname) desc, datname", matchdbre)
	if err != nil {
		return nil, nil, false, err
	}
	for r.Next() {
		var rownum int
		var datname string
		var initial bool
		err := r.Scan(&rownum, &datname, &initial)
		if err != nil {
			r.Close()
			return nil, nil, false, err
		}
		// -1 because postgres indexes arrays from 1, while Go counts from 0
		dbmatches[rownum-1][datname] = true
		if !matcheddbmap[datname] {
			matcheddblist = append(matcheddblist, datname)
		}
		if initial {
			initialmatch = true
		}
		matcheddbmap[datname] = true
	}
	r.Close()

	return dbmatches, matcheddblist, initialmatch, nil
}

func (i *DBInterface) GetTableMatches(datname string, matchconfig []matchType, rulesetconfig rulesetType) ([][][]table, error) {
	type MatchRule struct {
		Operator  string `json:"operator"`
		Threshold uint64 `json:"threshold"`
	}

	type MatchSection struct {
		DBRE     string      `json:"dbre"`
		SchemaRE string      `json:"schemare"`
		TableRE  string      `json:"tablere"`
		Rules    []MatchRule `json:"rules"`
	}

	// Initialize structure to hold results with capacities from input values
	tablematches := make([][][]table, cap(matchconfig))
	for idx, _ := range matchconfig {
		tablematches[idx] = make([][]table, cap(rulesetconfig[matchconfig[idx].Ruleset]))
	}

	rulesfordb := make([]MatchSection, 0, cap(matchconfig))
	for idx, val := range matchconfig {
		rulesfordb = append(rulesfordb, MatchSection{DBRE: val.Database, SchemaRE: val.Schema, TableRE: val.Table, Rules: make([]MatchRule, 0, cap(rulesetconfig[val.Ruleset]))})
		for _, val := range rulesetconfig[val.Ruleset] {
			rulesfordb[idx].Rules = append(rulesfordb[idx].Rules, MatchRule{Operator: val.Condition, Threshold: val.Value})
		}
	}
	buf, err := json.Marshal(rulesfordb)
	if err != nil {
		return nil, err
	}
	jsonfordb := string(buf)

	/*
		Here is where the black magic happens. We've coerced the configured rules
		into a json-encoded data structure. We pass this to the database and this
		query pulls it apart and joins against pg_class.

		The result is all matching tables in the database, first with the match
		section they matched, and then the rule they matched. Note that if a table
		matches a section, but does not match any rules within it, it will still not
		match subsequent sections.
	*/
	r, err := i.conn.Query(bgctx, `with jsonin as
  (select $1::jsonb as jsonin),
     tablesub as
  (select row_number() over () as tablematchnum,
                            dbre,
                            schemare,
                            tablere
   from
     (select jsonb_array_elements(jsonin)->>'dbre' as dbre,
                                            jsonb_array_elements(jsonin)->>'schemare' as schemare,
                                                                           jsonb_array_elements(jsonin)->>'tablere' as tablere
      from jsonin) tablesub1),
     rulessub as
  (select tablematchnum,
          row_number() over (partition by tablematchnum) as rulenum,
   operator,
                            threshold
   from
     (select row_number() over () as tablematchnum,
                               jsonb_array_elements(rules)->>'operator' as
      operator,
                                                             (jsonb_array_elements(rules)->>'threshold')::bigint as threshold
      from
        (select jsonb_array_elements(jsonin)->'rules' as rules
         from jsonin) rulessub1) rulessub2),
     tablefiltersub as
  (select tablematchnum,
          relnamespace,
          relname,
          reltuples
   from
     (select ts.tablematchnum,
             c.relnamespace::regnamespace::text as relnamespace,
             c.relname,
             min(ts.tablematchnum) over (partition by c.relnamespace,
                                                      c.relname) as mintablematchnum,
                                        c.reltuples
      from pg_class c
      join tablesub ts on c.relnamespace::regnamespace::text ~ ts.schemare
      and c.relname ~ ts.tablere
      where current_database() ~ ts.dbre
        and c.relpersistence='p'
        and c.relkind in ('r',
                          'm',
                          'p')) tablefiltersub1
   where tablematchnum = mintablematchnum),
     rulesfiltersub as
  (select tablematchnum,
          rulenum,
          relnamespace,
          relname
   from
     (select tfs.tablematchnum,
             rs.rulenum,
             tfs.relnamespace,
             tfs.relname,
             min(rulenum) over (partition by tfs.relnamespace,
                                             tfs.relname,
                                             tfs.tablematchnum) as minrulenum
      from tablefiltersub tfs
      join rulessub rs on tfs.tablematchnum = rs.tablematchnum
      and case
              when rs.operator = 'ge'
                   and tfs.reltuples >= rs.threshold then 't'::bool
              when rs.operator = 'lt'
                   and tfs.reltuples < rs.threshold then 't'::bool
              else 'f'::bool
          end) rulesfiltersub1
   where rulenum = minrulenum)
select tablematchnum,
       rulenum,
       relnamespace,
       relname,
       format('%I.%I', relnamespace, relname) as quotedfull
from rulesfiltersub
order by tablematchnum,
         rulenum,
         pg_table_size(format('%I.%I', relnamespace, relname)::regclass) desc`, jsonfordb)
	if err != nil {
		return nil, err
	}

	for r.Next() {
		var tablematchnum int
		var rulenum int
		var relnamespace string
		var relname string
		var quotedfull string

		err := r.Scan(&tablematchnum, &rulenum, &relnamespace, &relname, &quotedfull)
		if err != nil {
			r.Close()
			return nil, err
		}
		//fmt.Printf("Matched table %s with section %d, rule %d\n", quotedfull, tablematchnum, rulenum)

		tablematches[tablematchnum-1][rulenum-1] = append(tablematches[tablematchnum-1][rulenum-1], table{SchemaName: relnamespace, TableName: relname, QuotedFullName: quotedfull})
	}

	return tablematches, nil
}

/*
type Table

func (t *Table) GetOptions() TableOptions {
	datnames:=make([]string,0)
	r,err:=i.conn.Query(bgctx,"select relnamespace::regnamespace::text, relname, reloptions[1] as reloptname, reloptions[2] as reloptsetting from (select relnamespace,relname,regexp_split_to_array(unnest(reloptions),'=') as reloptions from pg_class where oid=$1) x",)
	if err != nil {
		panic(err)
	}
	{
		defer r.Close()
		for r.Next() {
			var s string
			err:=r.Scan(&s)
			if err != nil {
				panic(err)
			}
			datnames=append(datnames,s)
		}
	}
	return datnames
}
*/
