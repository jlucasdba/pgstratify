package main

import "context"
import "encoding/json"
import "errors"
import "fmt"
import "github.com/jackc/pgconn"
import "github.com/jackc/pgerrcode"
import "github.com/jackc/pgx/v4"
import "github.com/jlucasdba/pgvacman/queries"
import "regexp"
import "sort"
import "time"

const (
	WaitModeWait   = 1
	WaitModeNowait = 2
)

var bgctx = context.Background()

// Error indicating failure to acquire a lock
type AcquireLockError struct {
	Msg string
	Err error
}

func (e AcquireLockError) Error() string {
	return e.Msg
}

func (e AcquireLockError) Unwrap() error {
	return e.Err
}

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
	if r.Err() != nil {
		panic(r.Err())
	}
	return datnames
}

func (i *DBInterface) GetDBMatches(matchconfig []matchType) ([]string, bool, error) {
	matchdbre := make([]string, 0, 10)
	matcheddbmap := make(map[string]bool)
	matcheddblist := make([]string, 0)
	initialmatch := false

	for _, v := range matchconfig {
		matchdbre = append(matchdbre, v.Database)
	}
	r, err := i.conn.Query(bgctx, "select x.rownum, d.datname, case when d.datname=current_database() then 't'::bool else 'f'::bool end as initial from pg_database d join (select row_number() over () as rownum, re from (select unnest($1::text[]) as re) xx) x on d.datname ~ x.re where datallowconn='t' order by initial desc, pg_database_size(datname) desc, datname", matchdbre)
	if err != nil {
		return nil, false, err
	}
	for r.Next() {
		var rownum int
		var datname string
		var initial bool
		err := r.Scan(&rownum, &datname, &initial)
		if err != nil {
			r.Close()
			return nil, false, err
		}
		if !matcheddbmap[datname] {
			matcheddblist = append(matcheddblist, datname)
		}
		if initial {
			initialmatch = true
		}
		matcheddbmap[datname] = true
	}
	if r.Err() != nil {
		return nil, false, r.Err()
	}

	return matcheddblist, initialmatch, nil
}

func (i *DBInterface) GetTableMatches(datname string, matchconfig []matchType, rulesetconfig rulesetType) ([]tableMatch, error) {
	// define some structs for building json
	type Rule struct {
		Condition string            `json:"condition"`
		Value     uint64            `json:"value"`
		Set       map[string]string `json:"set"`
		Reset     []string          `json:"reset"`
	}

	type Ruleset []Rule

	type MatchSection struct {
		DBRE     string `json:"dbre"`
		SchemaRE string `json:"schemare"`
		TableRE  string `json:"tablere"`
		Ruleset  string `json:"ruleset"`
	}

	// define struct for parsing json from db
	type Option struct {
		OldSetting *string `json:"oldsetting"`
		NewSetting *string `json:"newsetting"`
	}

	// Initialize structure to hold results with capacities from input values
	tablematches := make([]tableMatch, 0)

	// Build data structures to be dumped to json for query input
	matchsectionsfordb := make([]MatchSection, 0, cap(matchconfig))
	for _, val := range matchconfig {
		matchsectionsfordb = append(matchsectionsfordb, MatchSection{DBRE: val.Database, SchemaRE: val.Schema, TableRE: val.Table, Ruleset: val.Ruleset})
	}
	rulesetsfordb := make(map[string]Ruleset, len(rulesetconfig))
	for key, val := range rulesetconfig {
		rulesetsfordb[key] = make(Ruleset, 0, cap(val))
		for idx2, val2 := range val {
			rulesetsfordb[key] = append(rulesetsfordb[key], Rule{Condition: val2.Condition, Value: val2.Value, Set: make(map[string]string, len(val2.Set)), Reset: make([]string, cap(val2.Reset))})
			for key3, val3 := range val2.Set {
				rulesetsfordb[key][idx2].Set[key3] = val3
			}
			for _, val3 := range val2.Reset {
				rulesetsfordb[key][idx2].Reset = append(rulesetsfordb[key][idx2].Reset, val3)
			}
		}
	}
	buf, err := json.Marshal(matchsectionsfordb)
	if err != nil {
		return nil, err
	}
	matchsectionsfordbjson := string(buf)
	buf, err = json.Marshal(rulesetsfordb)
	if err != nil {
		return nil, err
	}
	rulesetsfordbjson := string(buf)

	/*
		Here is where the black magic happens. We've coerced the configured rules
		into json-encoded data structures. We pass these to the database and this
		query pulls it apart and joins against pg_class.

		The result is all matching tables in the database that require at least one
		option update, with all the effective new settings. Note that if a table
		matches a section, but does not match any rules within it, it will still not
		match subsequent sections.
	*/
	r, err := i.conn.Query(bgctx, queries.RuleMatchQuery, matchsectionsfordbjson, rulesetsfordbjson)
	if err != nil {
		return nil, err
	}

	for r.Next() {
		var reloid int
		var quotedfullname string
		var jsonfromdb string

		err := r.Scan(&reloid, &quotedfullname, &jsonfromdb)
		if err != nil {
			r.Close()
			return nil, err
		}
		//fmt.Printf("Matched table %s with section %d, rule %d\n", quotedfull, tablematchnum, rulenum)

		options := make(map[string]Option)
		err = json.Unmarshal([]byte(jsonfromdb), &options)
		if err != nil {
			r.Close()
			return nil, err
		}
		tmoptions := make(map[string]tableMatchOption)
		for key, val := range options {
			tmoptions[key] = tableMatchOption(val)
		}
		tablematches = append(tablematches, tableMatch{Reloid: reloid, QuotedFullName: quotedfullname, Options: tmoptions})
	}
	if r.Err() != nil {
		return nil, r.Err()
	}

	return tablematches, nil
}

func (i *DBInterface) UpdateTableOptions(match tableMatch, dryrun bool, waitmode int, timeout float64) error {
	// Nearly all storage parameters don't actually require access
	// exclusive lock - if we are only setting such parameters, we
	// can use a less restrictive share update exclusive lock.
	// We evaluate whether we only have such parameters with a regexp.
	sharelockre, err := regexp.Compile(`autovacuum|(?:toast\.|^)(?:vacuum_|toast_|fillfactor$|parallel_workers$)`)
	usesharelock := true
	for key, _ := range match.Options {
		if !sharelockre.MatchString(key) {
			usesharelock = false
		}
	}

	var locksql string
	{
		lockmode := "access exclusive"
		if usesharelock {
			lockmode = "share update exclusive"
		}
		lockwait := ""
		if waitmode == WaitModeNowait {
			lockwait = " nowait"
		}
		locksql = fmt.Sprintf("lock table %s in %s mode%s", match.QuotedFullName, lockmode, lockwait)
	}

	tx, err := i.conn.BeginTx(bgctx, pgx.TxOptions{pgx.ReadCommitted, pgx.ReadWrite, pgx.NotDeferrable})
	if err != nil {
		return err
	}

	// launch goroutine that will wait for timeout and then cancel the lock attempt
	// lockdone will cancel the goroutine if the lock statement succeeds before
	// the timeout expires
	// we could use the context passed to query for timeout, but that kills
	// the db connection, which is inconvenient...
	lockdonectx, lockdone := context.WithCancel(bgctx)
	timeoutctx, timeoutcancel := context.WithTimeout(bgctx, DurationSeconds(timeout))
	go func() {
		defer timeoutcancel()

		select {
		case <-lockdonectx.Done():
			return
		case <-timeoutctx.Done():
			err := i.conn.PgConn().CancelRequest(bgctx)
			if err != nil {
				panic(err)
			}
			return
		}
	}()

	r, err := tx.Query(bgctx, locksql)
	lockdone()
	if err != nil {
		tx.Rollback(bgctx)
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == pgerrcode.LockNotAvailable {
			return &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s", match.QuotedFullName), err}
		} else if errors.Is(timeoutctx.Err(), context.DeadlineExceeded) && pgerr.Code == pgerrcode.QueryCanceled {
			return &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s (wait timed out)", match.QuotedFullName), err}
		} else {
			return err
		}
	}
	for r.Next() {
	}
	if r.Err() != nil {
		err := r.Err()
		tx.Rollback(bgctx)
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == pgerrcode.LockNotAvailable {
			return &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s", match.QuotedFullName), err}
		} else if errors.Is(timeoutctx.Err(), context.DeadlineExceeded) && pgerr.Code == pgerrcode.QueryCanceled {
			return &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s (wait timed out)", match.QuotedFullName), err}
		} else {
			return err
		}
	}

	// Now we cycle through the table options and try to set each one
	sortedkeys := make([]string, 0, len(match.Options))
	for key, _ := range match.Options {
		sortedkeys = append(sortedkeys, key)
	}
	sort.Strings(sortedkeys)
	for _, val := range sortedkeys {
		var altersql string
		if match.Options[val].NewSetting == nil {
			fmt.Printf("  Reset %s\n", val)
			altersql = fmt.Sprintf("alter table %s reset (%s)", match.QuotedFullName, val)
		} else if match.Options[val].OldSetting == nil {
			fmt.Printf("  Set %s to %s (previously unset)\n", val, *match.Options[val].NewSetting)
			altersql = fmt.Sprintf("alter table %s set (%s=%s)", match.QuotedFullName, val, *match.Options[val].NewSetting)
		} else {
			fmt.Printf("  Set %s to %s (previous setting %s)\n", val, *match.Options[val].NewSetting, *match.Options[val].OldSetting)
			altersql = fmt.Sprintf("alter table %s set (%s=%s)", match.QuotedFullName, val, *match.Options[val].NewSetting)
		}
		if !dryrun {
			tx2, err := tx.Begin(bgctx)
			if err != nil {
				panic(err)
			}
			r, err := tx2.Query(bgctx, altersql)
			for r.Next() {
			}
			if r.Err() != nil {
				err := r.Err()
				tx2.Rollback(bgctx)
				fmt.Printf("  Unable to set storage parameter %s: %v\n", val, err)
			}
			err = tx2.Commit(bgctx)
			if err != nil {
				panic(err)
			}
		}
	}

	err = tx.Commit(bgctx)
	if err != nil {
		tx.Rollback(bgctx)
		return err
	}
	return nil
}

// utility function returning a duration in seconds
func DurationSeconds(seconds float64) time.Duration {
	z := fmt.Sprintf("%fs", seconds)
	x, err := time.ParseDuration(z)
	if err != nil {
		// should never happen
		panic(err)
	}
	return x
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
