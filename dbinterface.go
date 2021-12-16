// Copyright (c) 2021 James Lucas

package main

import "context"
import "encoding/json"
import "errors"
import "fmt"
import "github.com/jackc/pgconn"
import "github.com/jackc/pgerrcode"
import "github.com/jackc/pgx/v4"
import "github.com/jlucasdba/pgvacman/queries"
import log "github.com/sirupsen/logrus"
import "regexp"
import "sort"
import "strings"

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

// Error indicating password authentication failure
type PasswordAuthenticationError struct {
	Err error
}

func (e PasswordAuthenticationError) Error() string {
	return e.Err.Error()
}

func (e PasswordAuthenticationError) Unwrap() error {
	return e.Err
}

// Struct wrapping a database connection.
type DBInterface struct {
	config *pgx.ConnConfig
	conn   *pgx.Conn
}

func NewDBInterface(connectoptions *ConnectOptions) (*DBInterface, error) {
	i := DBInterface{}
	var err error

	i.config, err = pgx.ParseConfig(connectoptions.BuildDSN())
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(bgctx, i.config)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == pgerrcode.InvalidPassword {
			return nil, &PasswordAuthenticationError{err}
		}
		return nil, err
	}
	i.conn = conn
	return &i, nil
}

func (i *DBInterface) Close() {
	i.conn.Close(bgctx)
}

func (i *DBInterface) ListDBs() []string {
	datnames := make([]string, 0)
	r, err := i.conn.Query(bgctx, "select datname from pg_database where datallowconn = 't'")
	if err != nil {
		log.Fatal(err)
	}
	for r.Next() {
		var s string
		err := r.Scan(&s)
		if err != nil {
			r.Close()
			log.Fatal(err)
		}
		datnames = append(datnames, s)
	}
	if r.Err() != nil {
		log.Fatal(r.Err())
	}
	return datnames
}

func (i *DBInterface) GetTableMatches(matchconfig []ConfigMatch, rulesetconfig ConfigRuleset) ([]TableMatch, error) {
	// define some structs for building json
	type Rule struct {
		Condition string            `json:"condition"`
		Value     uint64            `json:"value"`
		Set       map[string]string `json:"set"`
		Reset     []string          `json:"reset"`
	}

	type Ruleset []Rule

	type MatchSection struct {
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
	tablematches := make([]TableMatch, 0)

	// Build data structures to be dumped to json for query input
	matchsectionsfordb := make([]MatchSection, 0, cap(matchconfig))
	for _, val := range matchconfig {
		matchsectionsfordb = append(matchsectionsfordb, MatchSection{SchemaRE: val.Schema, TableRE: val.Table, Ruleset: val.Ruleset})
	}
	rulesetsfordb := make(map[string]Ruleset, len(rulesetconfig))
	for key, val := range rulesetconfig {
		rulesetsfordb[key] = make(Ruleset, 0, cap(val))
		for idx2, val2 := range val {
			rulesetsfordb[key] = append(rulesetsfordb[key], Rule{Condition: val2.Condition, Value: val2.Value, Set: make(map[string]string, len(val2.Set)), Reset: make([]string, 0, cap(val2.Reset))})
			for key3, val3 := range val2.Set {
				rulesetsfordb[key][idx2].Set[key3] = val3
			}
			rulesetsfordb[key][idx2].Reset = append(rulesetsfordb[key][idx2].Reset, val2.Reset...)
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
		into json-encoded data structures. We pass these to the database and build
		temporary tables, which are joined with catalog tables to find the matching
		tables and applicable rules.

		The result is all matching tables in the database that require at least one
		option update, with all the effective new settings. Note that if a table
		matches a section, but does not match any rules within it, it will still not
		match subsequent sections.
	*/
	tx, err := i.conn.BeginTx(bgctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return nil, err
	}
	// we don't need the temp tables after this transaction ends, and we're not writing, so rollback is fine
	defer func() {
		err := tx.Rollback(bgctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	_, err = tx.Exec(bgctx, queries.TablesTempTab, matchsectionsfordbjson)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(bgctx, queries.TablesTempTabPK)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(bgctx, `analyze pg_temp.tables`)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(bgctx, queries.TableOptionsTempTab)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(bgctx, queries.TableOptionsTempTabPK)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(bgctx, `analyze pg_temp.tableoptions`)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(bgctx, queries.RulesetsTempTab, rulesetsfordbjson)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(bgctx, queries.RulesetsTempTabPK)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(bgctx, queries.RulesetsSettingsTempTab, rulesetsfordbjson)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(bgctx, queries.RulesetsSettingsTempTabPK)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(bgctx, `analyze pg_temp.rulesets, pg_temp.rulesets_settings`)
	if err != nil {
		return nil, err
	}

	r, err := tx.Query(bgctx, queries.RuleMatchQuery)
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

		options := make(map[string]Option)
		err = json.Unmarshal([]byte(jsonfromdb), &options)
		if err != nil {
			r.Close()
			return nil, err
		}
		tmoptions := make(map[string]TableMatchOption)
		for key, val := range options {
			tmoptions[key] = TableMatchOption(val)
		}
		tablematches = append(tablematches, TableMatch{Reloid: reloid, QuotedFullName: quotedfullname, Options: tmoptions})
	}
	if r.Err() != nil {
		return nil, r.Err()
	}

	return tablematches, nil
}

type UpdateTableOptionsResultSettingSuccess struct {
	Setting string
	Success bool
}

type UpdateTableOptionsResult struct {
	Match          TableMatch
	SettingSuccess []UpdateTableOptionsResultSettingSuccess
}

func (i *DBInterface) UpdateTableOptions(match TableMatch, dryrun bool, waitmode int, timeout float64) (UpdateTableOptionsResult, error) {
	result := UpdateTableOptionsResult{Match: match, SettingSuccess: make([]UpdateTableOptionsResultSettingSuccess, len(match.Options))}

	// Nearly all storage parameters don't actually require access
	// exclusive lock - if we are only setting such parameters, we
	// can use a less restrictive share update exclusive lock.
	// We evaluate whether we only have such parameters with a regexp.
	sharelockre, err := regexp.Compile(`autovacuum|(?:toast\.|^)(?:vacuum_|toast_|fillfactor$|parallel_workers$)`)
	if err != nil {
		log.Panic(err)
	}
	usesharelock := true
	for key := range match.Options {
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

	tx, err := i.conn.BeginTx(bgctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return result, err
	}

	// we only need to set timeout in wait mode, and if timeout is zero or greater
	if waitmode == WaitModeWait && timeout >= 0 {
		_, err = tx.Exec(bgctx, fmt.Sprintf(`set lock_timeout = %s`, pgx.Identifier{fmt.Sprintf(`%fs`, timeout)}.Sanitize()), pgx.QuerySimpleProtocol(true))
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = tx.Exec(bgctx, locksql)
	if err != nil {
		rberr := tx.Rollback(bgctx)
		if rberr != nil {
			log.Fatal(rberr)
		}
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == pgerrcode.LockNotAvailable && !strings.Contains(pgerr.Error(), "timeout") {
			return result, &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s", match.QuotedFullName), err}
		} else if errors.As(err, &pgerr) && pgerr.Code == pgerrcode.LockNotAvailable {
			return result, &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s (wait timed out)", match.QuotedFullName), err}
		} else {
			return result, err
		}
	}

	// Now we cycle through the table options and try to set each one
	sortedkeys := make([]string, 0, len(match.Options))
	for key := range match.Options {
		sortedkeys = append(sortedkeys, key)
	}
	sort.Strings(sortedkeys)
	for _, val := range sortedkeys {
		var altersql string
		if match.Options[val].NewSetting == nil {
			altersql = fmt.Sprintf("alter table %s reset (%s)", match.QuotedFullName, pgx.Identifier{val}.Sanitize())
		} else if match.Options[val].OldSetting == nil {
			altersql = fmt.Sprintf("alter table %s set (%s=%s)", match.QuotedFullName, pgx.Identifier{val}.Sanitize(), pgx.Identifier{*match.Options[val].NewSetting}.Sanitize())
		} else {
			altersql = fmt.Sprintf("alter table %s set (%s=%s)", match.QuotedFullName, pgx.Identifier{val}.Sanitize(), pgx.Identifier{*match.Options[val].NewSetting}.Sanitize())
		}
		if !dryrun {
			tx2, err := tx.Begin(bgctx)
			if err != nil {
				log.Fatal(err)
			}
			_, err = tx2.Exec(bgctx, altersql, pgx.QuerySimpleProtocol(true))
			if err != nil {
				rberr := tx2.Rollback(bgctx)
				if rberr != nil {
					log.Fatal(rberr)
				}
				result.SettingSuccess = append(result.SettingSuccess, UpdateTableOptionsResultSettingSuccess{Setting: val, Success: false})
			} else {
				err = tx2.Commit(bgctx)
				if err != nil {
					log.Fatal(err)
				}
				result.SettingSuccess = append(result.SettingSuccess, UpdateTableOptionsResultSettingSuccess{Setting: val, Success: true})
			}
		} else {
			result.SettingSuccess = append(result.SettingSuccess, UpdateTableOptionsResultSettingSuccess{Setting: val, Success: true})
		}
	}

	err = tx.Commit(bgctx)
	if err != nil {
		rberr := tx.Rollback(bgctx)
		if rberr != nil {
			log.Fatal(rberr)
		}
		return result, err
	}
	return result, nil
}
