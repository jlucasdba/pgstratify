# pgvacman

Pgvacman is a storage parameter manager for PostgreSQL. Specifically it's intended target is managing the autovacuum-related storage parameters of large tables as they grow.

## Quickstart

You need to create a yaml file defining what tables to operate on (matchgroups), and what parameters to update on those tables at specific rowcount thresholds (rulesets). A simple example:

```yaml
matchgroups:
  - schema: ^public$
    table: .*
    owner: .*
    ruleset: set1
rulesets:
  set1:
    - minrows: 100000
      settings:
        autovacuum_vacuum_threshold: 20000
        autovacuum_vacuum_scale_factor: 0
        autovacuum_analyze_threshold: 10000
        autovacuum_analyze_scale_factor: 0
    - minrows: 0
      settings:
        autovacuum_vacuum_threshold:
        autovacuum_vacuum_scale_factor:
        autovacuum_analyze_threshold:
        autovacuum_analyze_scale_factor:
```

Matchgroups use [Postgres POSIX regular expressions](https://www.postgresql.org/docs/14/functions-matching.html#FUNCTIONS-POSIX-REGEXP) to select a set of tables. The rules in the rulesets associated with that matchgroup are then applied to that set of tables. In this simple case, we are saying "Match all tables in the public schema. For each table, if its rowcount is greater than or equal to 2000000, set these storage parameters on it. If its rowcount is greater than 0 (and less than 2000000), reset the values of the listed parameters on it."

You can do a dry-run of the tool against your database like this:
`pgvacman --database mydatabase --dry-run myconfig.yaml`

When you're ready to apply the changes, you can do this:
`pgvacman --database mydatabase --verbose myconfig.yaml`

The recommended usage, once your rules are satisfactorily defined, is to schedule pgvacman to run periodically in a cron job (or some other scheduling mechanism).

## Why Do I Need This?

If your database is relatively small, and/or the data in your tables is very stable, you probably don't. The main use case is to update table storage parameters when rowcounts get high enough that the default percentage-based autovacuum approach starts to break down.

## Detailed Rationale

In a Postgres database, the autovacuum daemon is the component of the system responsible for garbage collecting dead tuples in datafiles and freeing space for re-use. It also performs a number of other datafile hygiene functions, including gathering table optimizer statistics. There are a number of system-level settings related to tuning autovacuum, as well as table level storage parameters that can override the system settings. (See [here](https://www.postgresql.org/docs/14/routine-vacuuming.html#AUTOVACUUM) and [here](https://www.postgresql.org/docs/14/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS) for details.)

The problem is that settings that work well for small tables don't really work well for large tables, and vice-versa. For example, take the `autovacuum_vacuum_scale_factor` parameter. This defines a percentage threshold - once that percentage of tuples in a table have been modified, an autovacuum run is triggered. The default setting is 0.2, meaning 20%. For small to medium sized tables this works okay, but as rowcounts go up it starts to cause problems. If your table has 50,000 rows, 20% is 10,000 rows. If your table has 500,000,000 rows, 20% is 100,000,000 rows.

Postgres vacuum runs more quickly when it's run more frequently. The visibility map for each table keeps track of pages in need of vacuuming. When vacuum runs it can skip straight to these pages and bypass the clean pages. So the more pages in need of cleaning, the longer the vacuum takes. The more tuples a vacuum needs to process, the more memory it needs as well. A vacuum process starved for memory becomes even slower. This problem can snowball: tables that have a lot of activity and aren't vacuumed frequently enough become bloated. The more pages a table has, the longer it takes to scan, which leads to the next vacuum taking even longer... This can spiral out of control.

Pgvacman is designed to help you maintain sane autovacuum settings for tables of different sizes, with better granularity than the system-wide settings provide. The user defines bands of table rowcounts at which different table-level storage parameters will be set.  For example, say the database wide `autovacuum_vacuum_scale_factor` is .2, but you want any tables over 5,000,000 rows to instead use a value of .02. You can define a rule for this threshold in the pgvacman config file. When it runs, pgvacman will scan the database for tables of that size and modify their storage parameters to match. If the table later shrinks (after a truncate, for example), the next pgvacman run will revert the storage parameters to a lower configured band. You can keep whatever system-wide settings you are comfortable with for average tables, and define more aggressive settings for tables that reach defined rowcount cutoffs. You can also define different rules for specific schema or table name patterns if you know your environment has specific tables with special requirements.

## Command-line Reference

### Basic Usage
  `./pgvacman [OPTION] ... [RULEFILE]`

### Options:
`--display-matches`

Take no action, and display what tables matched each matchgroup. Useful for debugging configuration.

`-n, --dry-run`

Output what would be done without making changes (implies -v).

`-j, --jobs=NUM`
Use up to NUM concurrent connections to set storage parameters. This is primarily useful on busy systems where ALTER TABLE might be blocked. More connections allows more locks to be waited on simultaneously.

`--lock-timeout=NUM`

Per-table wait timeout in seconds (must be greater than 0, no effect in skip-locked mode). Wait at most this many seconds to acquire lock on a given table before giving up. If multiple connections are in use, more than one table may be waited on simultaneously.

`--skip-locked`

Skip tables that cannot be immediately locked.

`-v, --verbose`

Be more verbose about what is happening. Includes output of every table matched, what parameters are being modified, and old and new settings. Implied in dry-run mode.

`-?, --help`

Show help and exit.

### Connection Options:
`-h, --host=HOSTNAME`

Database server host or socket directory.

`-p, --port=PORT`

Database server port.

`-U, --username=USERNAME`

User name to connect as.

`-w, --no-password`

Never prompt for password.

`-W, --password`

Force password prompt.

`-d, --dbname`

Database name to connect to and update.

## YAML Configuration Reference

**matchgroups:** List of matchgroups - each matchgroup supports the following keys:
* schema: A postgres regular expression matching one or more schema names. Defaults to empty string, which matches all schemas.
* table: A postgres regular expression matching one or more table (or materialized view) names. Defaults to empty string, which matches all tables (and materialized views).
* owner: A postgres regular expression matching one or more table owners. Defaults to empty string, which matches any owner.
* case_sensitive: Boolean value, indicating whether name matching should be case sensitive for this matchgroup. Defaults to false.
* ruleset: A ruleset name from the rulesets section of the configuration. This is the ruleset that will be applied to tables matching this matchgroup. Defaults to empty string, meaning no ruleset will be applied to matched tables.

**rulesets:** Map of rulesets. The key for each ruleset is the ruleset name. Each ruleset consists of a list of rules. It is recommended, but not required, that the rules be specified in descending order, by their minrows value. Each rule consists of the following keys:
* minrows: The minimum number of rows a table must contain for this rule to apply. Defaults to 0, but relying on the default is not recommended. Two rules in the same ruleset cannot use the same minrows value.
* settings: Map of storage parameters to apply for this rule. The key is the parameter name, and the value is the setting. The default is null, meaning to RESET the parameter on the table.

All tables are checked against the matchgroup list in descending order. A table can match only one matchgroup - the first one for which it satisfies the matchgroup conditions. A table that has already matched a matchgroup is ignored by subsequent matchgroups.

For each table that matched a matchgroup, it is checked against the rules in the correspondig ruleset. The number of rows is determined from the optimizer statistics (reltuples in pg_class, specifically). All settings from rules with minrows less than or equal to the number of rows in the table apply. If a parameter is set in more than one appplicable rule, the setting from the rule with the highest minrows value applies. (In other words, settings from higher minrows rules mask settings from lower rules.)

## Recommendations

* Start simple. Setup a matchgroup to match all tables, and a rule to modify all tables over... say 100,000 rows. For example:
  ```yaml
  matchgroups:
    - schema: .*
      table: .*
      owner: .*
      ruleset: set1
  rulesets:
    set1:
      - minrows: 100000
        settings:
          autovacuum_vacuum_threshold: 20000
          autovacuum_vacuum_scale_factor: 0
          autovacuum_analyze_threshold: 10000
          autovacuum_analyze_scale_factor: 0
      - minrows: 0
        settings:
          autovacuum_vacuum_threshold:
          autovacuum_vacuum_scale_factor:
          autovacuum_analyze_threshold:
          autovacuum_analyze_scale_factor:
  ```
  This puts a cap on the autovacuum settings. After 100,000 rows, tables will be analyzed at 10000 rows modified, and vacuumed at 20000, no matter how big they get. If any tables drop back below 100,000, they will revert to the system settings. This is a starting point. You can adjust this configuration to meet your environment's needs. What size to make the cutoff at, and what threshold to use are very environment and hardware dependent, so it's impossible to make general recommendations. If you see tables where vacuum cycles are taking longer than a few minutes to run, those might be good candidates to run more often.

  For most cases, something pretty similar to this, run every day or so, is probably sufficient.
* You don't have to set a hard threshold.  You can stick with the percentage-based approach, but create size bands to gradually decrease the percentage. At 50000 rows, lower from .2 to .18, at 100000 lower to .15, etc. As long as you run pgvacman periodically to keep the settings up to date, this is fine.

## Tips & Tricks
* If you need to exclude certain tables from processing, you can put them in a matchgroup with an empty ruleset value. No assigned ruleset will mean no action taken against those tables, and once they have matched, they will be excluded from matching any later matchgroups.
* You're not limited to just autovacuum parameters - you can also change things like `parallel_workers` for large tables if you want.

## Caveats
* There's a tradeoff between vacuum frequency and vacuum duration. Vacuum too often and you're burning unneccessary cycles doing maintenance instead of serving queries. Vacuum too infrequently and vacuum can block needed structural changes, or get bogged down on a few large tables and never make it to the smaller tables. Pgvacman is intended to make managing these settings easier, but it's not a magic bullet.
* Rowcount isn't the only indicator that a table needs to be vacuumed more aggressively. Dirty page count (meaning pages with dead tuples) is also important in how long vacuum takes to run. Rowcount and dirty pages are related, but don't necessarily drectly correlate. The number of dirty pages is going to depend on the update pattern - it only takes one modified row to dirty a page. A table reaching a high percentage of dirty pages between vacuums probably should be vacuumed more often.
* It's possible for even a very small table (in terms of rowcount) to become a vacuum problem if it's very heavily updated/deleted from. For cases like this you may need define a special ruleset and target specific tables by name. In really bad cases, autovacuum may not be appropriate at all, and you may need to consider having your application do its own vacuuming.

Copyright (c) 2021 James Lucas
