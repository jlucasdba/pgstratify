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
	{
		defer r.Close()
		for r.Next() {
			var s string
			err := r.Scan(&s)
			if err != nil {
				panic(err)
			}
			datnames = append(datnames, s)
		}
	}
	return datnames
}

func (i *DBInterface) ListDBs() []string {
	datnames := make([]string, 0)
	r, err := i.conn.Query(context.Background(), "select datname from pg_database where datallowconn = 't'")
	if err != nil {
		panic(err)
	}
	{
		defer r.Close()
		for r.Next() {
			var s string
			err := r.Scan(&s)
			if err != nil {
				panic(err)
			}
			datnames = append(datnames, s)
		}
	}
	return datnames
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
