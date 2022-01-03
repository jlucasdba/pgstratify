// Copyright (c) 2022 James Lucas

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jlucasdba/pgvacman/queries"

	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

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

func (i *DBInterface) CurrentDB() string {
	var dbname string

	r, err := i.conn.Query(bgctx, "select current_database()")
	if err != nil {
		log.Fatal(err)
	}
	for r.Next() {
		err := r.Scan(&dbname)
		if err != nil {
			r.Close()
			log.Fatal(err)
		}
	}
	return dbname
}

func (i *DBInterface) GetTableMatches(matchconfig []ConfigMatchgroup, rulesetconfig map[string]ConfigRuleset) ([]TableMatch, error) {
	// define some structs for building json
	type Rule struct {
		Minrows  uint64             `json:"minrows"`
		Settings map[string]*string `json:"settings"`
	}

	type Ruleset []Rule

	type Matchgroup struct {
		SchemaRE      string `json:"schemare"`
		TableRE       string `json:"tablere"`
		OwnerRE       string `json:"ownerre"`
		CaseSensitive bool   `json:"case_sensitive"`
		Ruleset       string `json:"ruleset"`
	}

	// define struct for parsing json from db
	type Setting struct {
		OldSetting *string `json:"oldsetting"`
		NewSetting *string `json:"newsetting"`
	}

	// Initialize structure to hold results with capacities from input values
	tablematches := make([]TableMatch, 0)

	// Build data structures to be dumped to json for query input
	matchgroupsfordb := make([]Matchgroup, 0, len(matchconfig))
	for _, val := range matchconfig {
		matchgroupsfordb = append(matchgroupsfordb, Matchgroup{SchemaRE: val.Schema, TableRE: val.Table, OwnerRE: val.Owner, CaseSensitive: val.CaseSensitive, Ruleset: val.Ruleset})
	}
	rulesetsfordb := make(map[string]Ruleset, len(rulesetconfig))
	for key, val := range rulesetconfig {
		rulesetsfordb[key] = make(Ruleset, 0, len(val))
		for idx2, val2 := range val {
			rulesetsfordb[key] = append(rulesetsfordb[key], Rule{Minrows: val2.Minrows, Settings: make(map[string]*string, len(val2.Settings))})
			for key3, val3 := range val2.Settings {
				rulesetsfordb[key][idx2].Settings[key3] = val3
			}
		}
	}
	buf, err := json.Marshal(matchgroupsfordb)

	if err != nil {
		return nil, err
	}
	matchgroupsfordbjson := string(buf)
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

		In old versions this was all one giant query, but performance was inconsistent.
		Building the temp tables lets us gather stats (very helpful) and build indexes
		(dubiously helpful), at the cost of a litte extra work.
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

	/*
		Batch up the temp table creation statements for fewer roundtrips.
		Unfortunately they can't be all in the same batch because SELECT
		apparently can't reference tables created in the same batch.
		But we batch up as many as we can.
	*/
	var b pgx.Batch

	b.Queue(queries.TablesTempTab, matchgroupsfordbjson)
	b.Queue(queries.TablesTempTabPK)
	b.Queue(`analyze pg_temp.tables`)

	bresult := tx.SendBatch(bgctx, &b)
	for i := 0; i < b.Len(); i++ {
		_, err := bresult.Exec()
		if err != nil {
			bresult.Close()
			return nil, err
		}
	}
	err = bresult.Close()
	if err != nil {
		return nil, err
	}

	/*
		No batch for this one because no accompanying statements.
		Because of how this table is used, it won't really benefit
		from stats or an index.
	*/
	_, err = tx.Exec(bgctx, queries.RulesetsSubTempTab, rulesetsfordbjson)
	if err != nil {
		return nil, err
	}

	b = *new(pgx.Batch)

	b.Queue(queries.TableOptionsTempTab)
	b.Queue(queries.TableOptionsTempTabPK)
	b.Queue(`analyze pg_temp.tableoptions`)
	b.Queue(queries.RulesetsTempTab)
	b.Queue(queries.RulesetsTempTabPK)
	b.Queue(queries.RulesetsSettingsTempTab)
	b.Queue(queries.RulesetsSettingsTempTabPK)
	b.Queue(`analyze pg_temp.rulesets, pg_temp.rulesets_settings`)

	bresult = tx.SendBatch(bgctx, &b)
	for i := 0; i < b.Len(); i++ {
		_, err := bresult.Exec()
		if err != nil {
			bresult.Close()
			return nil, err
		}
	}
	bresult.Close()
	if err != nil {
		return nil, err
	}

	r, err := tx.Query(bgctx, queries.RuleMatchQuery)
	if err != nil {
		return nil, err
	}

	for r.Next() {
		var reloid int
		var relkind rune
		var quotedfullname string
		var owner string
		var reltuples int
		var jsonfromdb string
		var matchgroupidx int

		err := r.Scan(&reloid, &relkind, &quotedfullname, &owner, &reltuples, &jsonfromdb, &matchgroupidx)
		if err != nil {
			r.Close()
			return nil, err
		}

		options := make(map[string]Setting)
		err = json.Unmarshal([]byte(jsonfromdb), &options)
		if err != nil {
			r.Close()
			return nil, err
		}
		tmoptions := make(map[string]TableMatchOption)
		for key, val := range options {
			tmoptions[key] = TableMatchOption(val)
		}
		tablematches = append(tablematches, TableMatch{Reloid: reloid, Relkind: relkind, QuotedFullName: quotedfullname, Owner: owner, Reltuples: reltuples, MatchgroupNum: matchgroupidx, Matchgroup: &matchconfig[matchgroupidx-1], Options: tmoptions})
	}
	if r.Err() != nil {
		return nil, r.Err()
	}

	return tablematches, nil
}

type UpdateTableOptionsResultSettingSuccess struct {
	Setting string
	Success bool
	Err     error
}

type UpdateTableOptionsResult struct {
	Match          TableMatch
	SettingSuccess []UpdateTableOptionsResultSettingSuccess
}

func (i *DBInterface) UpdateTableOptions(match TableMatch, dryrun bool, waitmode int, timeout float64) (UpdateTableOptionsResult, error) {
	result := UpdateTableOptionsResult{Match: match, SettingSuccess: make([]UpdateTableOptionsResultSettingSuccess, 0, len(match.Options))}

	// dryrun case is much shorter, so get it out of the way upfront
	if dryrun {
		sortedkeys := make([]string, 0, len(match.Options))
		for key := range match.Options {
			sortedkeys = append(sortedkeys, key)
		}
		sort.Strings(sortedkeys)
		for _, val := range sortedkeys {
			result.SettingSuccess = append(result.SettingSuccess, UpdateTableOptionsResultSettingSuccess{Setting: val, Success: true})
		}
		return result, nil
	}

	tx, err := i.conn.BeginTx(bgctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return result, err
	}

	if waitmode == WaitModeNowait {
		// we simulate nowait by setting lock_timeout to 1ms (0 means wait forever)
		_, err = tx.Exec(bgctx, `set lock_timeout = 1`, pgx.QuerySimpleProtocol(true))
		if err != nil {
			log.Fatal(err)
		}
	}

	/*
		Because different parameters require different lock types, and can't know for
		sure what the highest level lock we need is (may change between database releases),
		each alter statement could potentially block. So we have to set a new timeout for
		each one, and fail the whole operation if we reach our deadline.
	*/
	var timeoutduration time.Duration
	var deadline time.Time
	if waitmode == WaitModeWait && timeout > 0 {
		timeoutduration = time.Duration(timeout * float64(time.Second))
		deadline = time.Now().Add(timeoutduration)
	}

	// string specifying if this is a table or materialized view
	objecttype, err := match.RelkindString()
	if err != nil {
		log.Fatal(err)
	}
	objecttype = strings.ToLower(objecttype)

	// Now we cycle through the table options and try to set each one
	sortedkeys := make([]string, 0, len(match.Options))
	for key := range match.Options {
		sortedkeys = append(sortedkeys, key)
	}
	sort.Strings(sortedkeys)
	for _, val := range sortedkeys {
		var altersql string
		if match.Options[val].NewSetting == nil {
			altersql = fmt.Sprintf("alter %s %s reset (%s)", objecttype, match.QuotedFullName, pgx.Identifier{val}.Sanitize())
		} else if match.Options[val].OldSetting == nil {
			altersql = fmt.Sprintf("alter %s %s set (%s=%s)", objecttype, match.QuotedFullName, pgx.Identifier{val}.Sanitize(), pgx.Identifier{*match.Options[val].NewSetting}.Sanitize())
		} else {
			altersql = fmt.Sprintf("alter %s %s set (%s=%s)", objecttype, match.QuotedFullName, pgx.Identifier{val}.Sanitize(), pgx.Identifier{*match.Options[val].NewSetting}.Sanitize())
		}
		tx2, err := tx.Begin(bgctx)
		if err != nil {
			log.Fatal(err)
		}
		if waitmode == WaitModeWait && timeout > 0 {
			remaining := time.Until(deadline).Milliseconds()
			if remaining > 0 {
				// lock_timeout for next alter is time remaining until deadline
				_, err = tx2.Exec(bgctx, fmt.Sprintf("set lock_timeout = %d", remaining), pgx.QuerySimpleProtocol(true))
			} else {
				// don't wait anymore - any further lock timeouts cause failure
				_, err = tx2.Exec(bgctx, "set lock_timeout = 1", pgx.QuerySimpleProtocol(true))
			}
			if err != nil {
				log.Fatal(err)
			}
		}
		_, err = tx2.Exec(bgctx, altersql, pgx.QuerySimpleProtocol(true))
		if err != nil {
			var pgerr *pgconn.PgError
			if errors.As(err, &pgerr) && pgerr.Code == pgerrcode.LockNotAvailable {
				// we fail the whole operation in this case - rollback main transaction
				rberr := tx.Rollback(bgctx)
				if rberr != nil {
					log.Fatal(rberr)
				}
				// return an empty result
				result := UpdateTableOptionsResult{Match: match, SettingSuccess: make([]UpdateTableOptionsResultSettingSuccess, 0)}
				if waitmode == WaitModeNowait {
					// we were blocked in nowait mode
					return result, &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s", match.QuotedFullName), err}
				} else {
					// our timeout expired
					return result, &AcquireLockError{fmt.Sprintf("Unable to acquire lock on %s (wait timed out)", match.QuotedFullName), err}
				}
			}

			/*
				If we got to here, we didn't timeout, we just failed to set the parameter.
				Rollback to the savepoint, record the error in result, and proceed.
			*/
			rberr := tx2.Rollback(bgctx)
			if rberr != nil {
				log.Fatal(rberr)
			}
			result.SettingSuccess = append(result.SettingSuccess, UpdateTableOptionsResultSettingSuccess{Setting: val, Success: false, Err: err})
		} else {
			// we succeeded in setting the parameter, so release the savepoint
			err = tx2.Commit(bgctx)
			if err != nil {
				log.Fatal(err)
			}
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
