#!/usr/bin/env python3

# Copyright (c) 2022 James Lucas

import subprocess

# Example script for running pgvacman against all databases in a cluster.
# Modify to suit your needs.

# the default config file to use for most databases
defaultconfig='pgvacman.yml'
# specific config files for specific named databases
dbconfigs={
  'postgres': 'postgres.yml'
}

r=subprocess.run(['psql','-t','-c','select datname from pg_database where datallowconn and not datistemplate'],stdout=subprocess.PIPE)
dbs=r.stdout.decode().split('\n')

# cleanup the results from the query
def reverseidx():
  i=len(dbs)-1
  while i >= 0:
    yield i
    i-=1

for x in reverseidx():
  dbs[x]=dbs[x].lstrip()
  if dbs[x] == '':
    del dbs[x]

# cycle through the database names
for db in dbs:
  # decide whether to use the default or specific config
  if dbconfigs.get(db) is not None:
    dbconfig=dbconfigs[db]
  else:
    dbconfig=defaultconfig
  # run pgvacman
  r=subprocess.run(['pgvacman','-d',db,'-v',dbconfig])
