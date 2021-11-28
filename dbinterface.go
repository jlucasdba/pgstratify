package main

import "context"
import "github.com/jackc/pgx/v4"

type DBInterface struct {
	dsn  string
	conn *pgx.Conn
}

func NewDBInterface(dsn string) DBInterface {
	i := DBInterface{}
	i.dsn = dsn
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		panic(err)
	}
	i.conn = conn
	return i
}

func (i *DBInterface) Close() {
	i.conn.Close(context.Background())
}

func (i *DBInterface) ListDBs() []string {
	datnames := make([]string, 0)
	r, err := i.conn.Query(context.Background(), "select datname from pg_database where datallowconn = 't'")
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

func (i *DBInterface) GetDBMatches(matchconfig []matchType) ([]realMatch, []string, error) {
	matchdbre := make([]string, 0, 10)
	realmatches := make([]realMatch, 0)
	matcheddbmap := make(map[string]bool)

	for _, v := range matchconfig {
		matchdbre = append(matchdbre, v.Database)
		realmatches = append(realmatches, newRealMatch())
	}
	r, err := i.conn.Query(context.Background(), "select x.rownum, d.datname from pg_database d join (select row_number() over () as rownum, re from (select unnest($1::text[]) as re) xx) x on d.datname ~ x.re where datallowconn='t' order by rownum, pg_database_size(datname) desc, datname", matchdbre)
	if err != nil {
		return nil, nil, err
	}
	for r.Next() {
		var rownum int
		var datname string
		err := r.Scan(&rownum, &datname)
		if err != nil {
			r.Close()
			return nil, nil, err
		}
		// -1 because postgres indexes arrays from 1, while Go counts from 0
		realmatches[rownum-1].Databases[datname] = true
		matcheddbmap[datname] = true
	}
	r.Close()

	matcheddbarray := make([]string, len(matcheddbmap))
	idx := 0
	for k, _ := range matcheddbmap {
		matcheddbarray[idx] = k
		idx++
	}
	return realmatches, matcheddbarray, nil
}

/*
type Table

func (t *Table) GetOptions() TableOptions {
	datnames:=make([]string,0)
	r,err:=i.conn.Query(context.Background(),"select relnamespace::regnamespace::text, relname, reloptions[1] as reloptname, reloptions[2] as reloptsetting from (select relnamespace,relname,regexp_split_to_array(unnest(reloptions),'=') as reloptions from pg_class where oid=$1) x",)
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
