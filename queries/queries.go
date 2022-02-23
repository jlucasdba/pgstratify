// Copyright (c) 2022 James Lucas

package queries

const TablesTempTab string = `create temporary table tables as
with matchjsonin as (select $1::jsonb as matchjsonin),
tables_sub1 as (select row_number() over () as tablematchnum, schemare, tablere, ownerre, case_sensitive, ruleset from (select jsonb_array_elements(matchjsonin)->>'schemare' as schemare, jsonb_array_elements(matchjsonin)->>'tablere' as tablere, jsonb_array_elements(matchjsonin)->>'ownerre' as ownerre, (jsonb_array_elements(matchjsonin)->>'case_sensitive')::boolean as case_sensitive, jsonb_array_elements(matchjsonin)->>'ruleset' as ruleset from matchjsonin) tables_sub1a)
select tablematchnum, reloid, relnamespace, relname, owner, reltuples, relkind, ruleset from (select ts1.tablematchnum, c.oid as reloid, c.relnamespace::regnamespace::text as relnamespace, c.relname, c.relowner::regrole::text as owner, min(ts1.tablematchnum) over (partition by c.relnamespace, c.relname) as mintablematchnum, c.reltuples, c.relkind, ts1.ruleset from pg_class c join tables_sub1 ts1 on (not ts1.case_sensitive and c.relnamespace::regnamespace::text ~* ts1.schemare and c.relname ~* ts1.tablere and c.relowner::regrole::text ~* ts1.ownerre) or (ts1.case_sensitive and c.relnamespace::regnamespace::text ~ ts1.schemare and c.relname ~ ts1.tablere and c.relowner::regrole::text ~ ts1.ownerre) where c.relpersistence='p' and c.relkind in ('r','m')) tables_a where tablematchnum = mintablematchnum`

const TablesTempTabPK string = `alter table pg_temp.tables add constraint pk_tables primary key (tablematchnum, reloid)`

const TableParametersTempTab string = `create temporary table tableparameters as
select reloid, reloptions[1] as parameter, reloptions[2] as setting from (select oid as reloid, regexp_split_to_array(unnest(reloptions),'=') as reloptions from pg_class where oid in (select reloid from pg_temp.tables)) tableparameters_a`

const TableParametersTempTabPK string = `alter table pg_temp.tableparameters add constraint pk_tableparameters primary key (reloid, parameter) include (setting)`

const RulesetsSubTempTab string = `create temporary table rulesets_sub as
with rulesetsjsonin as (select $1::jsonb as rulesetsjsonin),
rulesets_sub1 as (select key as ruleset, value from jsonb_each((select rulesetsjsonin from rulesetsjsonin)))
select ruleset, row_number() over (partition by ruleset order by minrows asc) as rulenum, minrows, settingsjson from (select ruleset, (value->>'minrows')::bigint as minrows, value->'settings' as settingsjson from (select ruleset, jsonb_array_elements(value) as value from rulesets_sub1) sub_a) sub_b`

const RulesetsTempTab string = `create temporary table rulesets as
select ruleset, rulenum, minrows from pg_temp.rulesets_sub`

const RulesetsTempTabPK string = `alter table pg_temp.rulesets add constraint pk_rulesets primary key (ruleset, rulenum) include (minrows)`

const RulesetsSettingsTempTab string = `create temporary table rulesets_settings as
select ruleset, rulenum, parameter, settingsjson->>parameter as setting from (select ruleset, rulenum, settingsjson, jsonb_object_keys(settingsjson) as parameter from pg_temp.rulesets_sub) sub`

const RulesetsSettingsTempTabPK string = `alter table pg_temp.rulesets_settings add constraint pk_rulesets_settings primary key (ruleset, rulenum, parameter) include (setting)`

const RuleMatchQuery string = `with rulematch as (select rs.ruleset, t.tablematchnum, rs.rulenum, t.reloid, t.relnamespace, t.relname, t.owner, t.reltuples, t.relkind from pg_temp.tables t join pg_temp.rulesets rs on t.ruleset = rs.ruleset and case
when t.reltuples >= rs.minrows then 't'::bool
else 'f'::bool end),
effective_settings_sub1 as (select rm.tablematchnum, rm.rulenum, rm.reloid, rm.relnamespace, rm.relname, rm.owner, rm.reltuples, rm.relkind, rss.parameter, rss.setting from rulematch rm join pg_temp.rulesets_settings rss on rm.ruleset = rss.ruleset and rm.rulenum=rss.rulenum),
effective_settings_sub2 as (select reloid, relnamespace, relname, owner, reltuples, relkind, tablematchnum, parameter, setting from effective_settings_sub1 where (tablematchnum, rulenum, reloid, relnamespace, relname, owner, parameter) in (select tablematchnum, max(rulenum) as rulenum, reloid, relnamespace, relname, owner, parameter from effective_settings_sub1 group by tablematchnum, reloid, relnamespace, relname, owner, parameter)),
effective_settings as (select ess.reloid, ess.relnamespace, ess.relname, ess.owner, ess.reltuples, ess.relkind, ess.tablematchnum, ess.parameter, tparams.setting as oldsetting, ess.setting as newsetting from effective_settings_sub2 ess left outer join tableparameters tparams on ess.reloid=tparams.reloid and ess.parameter=tparams.parameter where (ess.setting is null and (ess.reloid, ess.parameter) in (select reloid, parameter from tableparameters)) or (ess.setting is not null and (ess.reloid, ess.parameter, ess.setting) not in (select reloid, parameter, setting from tableparameters)))
select reloid::integer, relkind, format('%I.%I',relnamespace,relname) as quotedfullname, owner, reltuples, jsonout, tablematchnum from (select reloid, relnamespace, relname, owner, reltuples, relkind, tablematchnum, json_object_agg(parameter, json_build_object('oldsetting',oldsetting,'newsetting',newsetting)) as jsonout from effective_settings group by reloid, relnamespace, relname, owner, reltuples, relkind, tablematchnum order by relnamespace, relname, owner) sub`

const RuleMatchDisplayModeQuery string = `with rulematch as (select rs.ruleset, t.tablematchnum, rs.rulenum, t.reloid, t.relnamespace, t.relname, t.owner, t.reltuples, t.relkind from pg_temp.tables t join pg_temp.rulesets rs on t.ruleset = rs.ruleset and case
when t.reltuples >= rs.minrows then 't'::bool
else 'f'::bool end),
effective_settings_sub1 as (select rm.tablematchnum, rm.rulenum, rm.reloid, rm.relnamespace, rm.relname, rm.owner, rm.reltuples, rm.relkind, rss.parameter, rss.setting from rulematch rm join pg_temp.rulesets_settings rss on rm.ruleset = rss.ruleset and rm.rulenum=rss.rulenum),
effective_settings_sub2 as (select reloid, relnamespace, relname, owner, reltuples, relkind, tablematchnum, parameter, setting from effective_settings_sub1 where (tablematchnum, rulenum, reloid, relnamespace, relname, owner, parameter) in (select tablematchnum, max(rulenum) as rulenum, reloid, relnamespace, relname, owner, parameter from effective_settings_sub1 group by tablematchnum, reloid, relnamespace, relname, owner, parameter)),
effective_settings as (select ess.reloid, ess.relnamespace, ess.relname, ess.owner, ess.reltuples, ess.relkind, ess.tablematchnum, ess.parameter, tparams.setting as oldsetting, ess.setting as newsetting from effective_settings_sub2 ess left outer join tableparameters tparams on ess.reloid=tparams.reloid and ess.parameter=tparams.parameter)
select reloid::integer, relkind, format('%I.%I',relnamespace,relname) as quotedfullname, owner, reltuples, jsonout, tablematchnum from (select reloid, relnamespace, relname, owner, reltuples, relkind, tablematchnum, json_object_agg(parameter, json_build_object('oldsetting',oldsetting,'newsetting',newsetting)) as jsonout from effective_settings group by reloid, relnamespace, relname, owner, reltuples, relkind, tablematchnum order by relnamespace, relname, owner) sub`
