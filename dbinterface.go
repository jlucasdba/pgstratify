package main

import "context"
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
