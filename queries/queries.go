// Copyright (c) 2021 James Lucas

package queries

const TablesTempTab string = `create temporary table tables as
with matchjsonin as (select $1::jsonb as matchjsonin),
	tables_sub1 as (select row_number() over () as tablematchnum, schemare, tablere, ruleset from (select jsonb_array_elements(matchjsonin)->>'schemare' as schemare, jsonb_array_elements(matchjsonin)->>'tablere' as tablere, jsonb_array_elements(matchjsonin)->>'ruleset' as ruleset from matchjsonin) tables_sub1a),
tables as (select tablematchnum, reloid, relnamespace, relname, reltuples, ruleset from (select ts1.tablematchnum, c.oid as reloid, c.relnamespace::regnamespace::text as relnamespace, c.relname, min(ts1.tablematchnum) over (partition by c.relnamespace, c.relname) as mintablematchnum, c.reltuples, ts1.ruleset from pg_class c join tables_sub1 ts1 on c.relnamespace::regnamespace::text ~ ts1.schemare and c.relname ~ ts1.tablere where c.relpersistence='p' and c.relkind in ('r','m','p')) tables_a where tablematchnum = mintablematchnum)
select * from tables`

const TablesTempTabPK string = `alter table pg_temp.tables add constraint pk_tables primary key (tablematchnum, reloid)`

const TableOptionsTempTab string = `create temporary table tableoptions as
	with tableoptions as (select reloid, reloptions[1] as option, reloptions[2] as setting from (select oid as reloid, regexp_split_to_array(unnest(reloptions),'=') as reloptions from pg_class where oid in (select reloid from pg_temp.tables)) tableoptions_a)
	select * from tableoptions`

const TableOptionsTempTabPK string = `alter table pg_temp.tableoptions add constraint pk_tableoptions primary key (reloid, option) include (setting)`

const RulesetsTempTab string = `create temporary table rulesets as
with rulesetsjsonin as (select $1::jsonb as rulesetsjsonin),
	rulesets_sub1 as (select key as ruleset, value from jsonb_each((select rulesetsjsonin from rulesetsjsonin))),
	rulesets_sub2 as (select ruleset, row_number() over (partition by ruleset) as rulenum, operator, threshold, settingjson, resetjson from (select ruleset, value->>'condition' as operator, (value->>'value')::bigint as threshold, value->'set' as settingjson, value->'reset' as resetjson from (select ruleset, jsonb_array_elements(value) as value from rulesets_sub1) rulesets_sub2a) rulesets_sub2b),
	rulesets as (select ruleset, rulenum, operator, threshold from rulesets_sub2)
select * from rulesets`

const RulesetsTempTabPK string = `alter table pg_temp.rulesets add constraint pk_rulesets primary key (ruleset, rulenum)`

const RulesetsSettingsTempTab string = `create temporary table rulesets_settings as
with rulesetsjsonin as (select $1::jsonb as rulesetsjsonin),
	rulesets_sub1 as (select key as ruleset, value from jsonb_each((select rulesetsjsonin from rulesetsjsonin))),
	rulesets_sub2 as (select ruleset, row_number() over (partition by ruleset) as rulenum, operator, threshold, settingjson, resetjson from (select ruleset, value->>'condition' as operator, (value->>'value')::bigint as threshold, value->'set' as settingjson, value->'reset' as resetjson from (select ruleset, jsonb_array_elements(value) as value from rulesets_sub1) rulesets_sub2a) rulesets_sub2b),
	rulesets_sub3 as (select ruleset, rulenum, operator, threshold, option, settingjson->>option as setting from (select ruleset, rulenum, operator, threshold, settingjson, jsonb_object_keys(settingjson) as option from rulesets_sub2) rulesets_sub3a),
	rulesets_sub4 as (select ruleset, rulenum, operator, threshold, jsonb_array_elements_text(resetjson) as option, null as setting from rulesets_sub2),
	rulesets_settings as (select ruleset, rulenum, option, setting from rulesets_sub4 union all (select ruleset, rulenum, option, setting from rulesets_sub3 where (ruleset, rulenum, operator, threshold, option) not in (select ruleset, rulenum, operator, threshold, option from rulesets_sub4)))
select * from rulesets_settings`

const RulesetsSettingsTempTabPK string = `alter table pg_temp.rulesets_settings add constraint pk_rulesets_settings primary key (ruleset, rulenum, option) include (setting)`

const RuleMatchQuery string = `with rulematch as (select rs.ruleset, t.tablematchnum, rs.rulenum, t.reloid, t.relnamespace, t.relname from pg_temp.tables t join pg_temp.rulesets rs on t.ruleset = rs.ruleset and case
	  when rs.operator = 'ge' and t.reltuples >= rs.threshold then 't'::bool
	  when rs.operator = 'lt' and t.reltuples < rs.threshold then 't'::bool
	  else 'f'::bool end),
	effective_settings_sub1 as (select rm.tablematchnum, rm.rulenum, rm.reloid, rm.relnamespace, rm.relname, rss.option, rss.setting from rulematch rm join pg_temp.rulesets_settings rss on rm.ruleset = rss.ruleset and rm.rulenum=rss.rulenum),
	--effective_settings_sub2 as (select reloid, relnamespace, relname, option, setting from effective_settings_sub1 where (tablematchnum, rulenum, reloid, relnamespace, relname, option) in (select tablematchnum, max(rulenum) as rulenum, reloid, relnamespace, relname, option from effective_settings_sub1 group by tablematchnum, reloid, relnamespace, relname, option) and ((setting is null and (reloid, option) in (select reloid, option from pg_temp.tableoptions)) or (setting is not null and (reloid, option, setting) not in (select reloid, option, setting from pg_temp.tableoptions)))),
	--effective_settings as (select ess.reloid, ess.relnamespace, ess.relname, ess.option, topt.setting as oldsetting, ess.setting as newsetting from effective_settings_sub2 ess left outer join tableoptions topt on ess.reloid=topt.reloid and ess.option=topt.option)
	effective_settings_sub2 as (select reloid, relnamespace, relname, option, setting from effective_settings_sub1 where (tablematchnum, rulenum, reloid, relnamespace, relname, option) in (select tablematchnum, max(rulenum) as rulenum, reloid, relnamespace, relname, option from effective_settings_sub1 group by tablematchnum, reloid, relnamespace, relname, option)),
	effective_settings as (select ess.reloid, ess.relnamespace, ess.relname, ess.option, topt.setting as oldsetting, ess.setting as newsetting from effective_settings_sub2 ess left outer join tableoptions topt on ess.reloid=topt.reloid and ess.option=topt.option where (ess.setting is null and (ess.reloid, ess.option) in (select reloid, option from tableoptions)) or (ess.setting is not null and (ess.reloid, ess.option, ess.setting) not in (select reloid, option, setting from tableoptions)))
	select reloid::integer, format('%I.%I',relnamespace,relname) as quotedfullname, jsonout from (select reloid, relnamespace, relname, json_object_agg(option, json_build_object('oldsetting',oldsetting,'newsetting',newsetting)) as jsonout from effective_settings group by reloid, relnamespace, relname order by relnamespace, relname) sub`
