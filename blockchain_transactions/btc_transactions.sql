create database if not exists btc;
use btc;

drop table if exists btc.transactions;
CREATE TABLE if not exists btc.transactions
(
    hash                          UInt256                   CODEC(ZSTD(1)),
    block_hash                    UInt256                   CODEC(ZSTD(1)),
    block_number                  UInt32                    CODEC(ZSTD(1)),
    `index`                       UInt16                    CODEC(ZSTD(1)),
    block_timestamp               DateTime                  CODEC(ZSTD(1)),
    date                          ALIAS     toDate(block_timestamp),
    lock_time                     UInt64                    CODEC(ZSTD(1)),
    output_value                  Float64                   CODEC(ZSTD(1)),
    fee                           Float64                   CODEC(ZSTD(1)),
    version                       UInt8                     CODEC(ZSTD(1)),
    is_coinbase                   Bool                      CODEC(ZSTD(1)),
    size                          UInt32                    CODEC(ZSTD(1)),
    virtual_size                  UInt32                    CODEC(ZSTD(1)),
    input_count                   ALIAS     length(`inputs.required_signatures`),
    output_count                  ALIAS     length(`outputs.required_signatures`),
    `outputs.address`             Array(UInt64)             CODEC(ZSTD(1)),
    `outputs.index`               Array(UInt16)             CODEC(ZSTD(1)),
    `outputs.required_signatures` Array(UInt8)              CODEC(ZSTD(1)),
    `outputs.script_id`           Array(UInt64)             CODEC(ZSTD(1)),
    `outputs.type`                Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `outputs.value`               Array(Float64)            CODEC(ZSTD(1)),
    `input_value`                 Float64                   CODEC(ZSTD(1)),
    `inputs.address`              Array(UInt64)             CODEC(ZSTD(1)),
    `inputs.index`                Array(UInt16)             CODEC(ZSTD(1)),
    `inputs.required_signatures`  Array(UInt8)              CODEC(ZSTD(1)),
    `inputs.script_id`            Array(UInt64)             CODEC(ZSTD(1)),
    `inputs.sequence`             Array(UInt64)             CODEC(ZSTD(1)),
    `inputs.spent_output_index`   Array(UInt16)             CODEC(ZSTD(1)),
    `inputs.spent_transaction_hash` Array(UInt256)          CODEC(ZSTD(1)),
    `inputs.txinwitness_id`       Array(Array(UInt64))      CODEC(ZSTD(1)),
    `inputs.type`                 Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `inputs.value`                Array(Float64)            CODEC(ZSTD(1)),
    last_modified                 Nullable(DateTime64(9))   CODEC(ZSTD(1))
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(toDate(block_timestamp))
        ORDER BY (toDate(block_timestamp), block_hash, hash)  -- todo: do we need block_hash for anything?
        SETTINGS non_replicated_deduplication_window = 1000;


drop table if exists btc.wallets;
create table if not exists btc.wallets
(
    id           UInt64,
    input_value  SimpleAggregateFunction(sum,Float64),
    input_count  SimpleAggregateFunction(sum,Float64),
    output_value SimpleAggregateFunction(sum,Float64),
    output_count SimpleAggregateFunction(sum,Float64),
    orig         SimpleAggregateFunction(groupUniqArrayArray,Array(String))
) ENGINE = AggregatingMergeTree
order by id
partition by id % 32
;
drop table if exists btc.witnesses;
create table if not exists btc.witnesses
(
    id          UInt64 CODEC(ZSTD(1)),
    bin         String CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree
order by (id,bin)
primary key id
partition by id % 32
;
drop table if exists btc.scripts;
create table if not exists btc.scripts
(
    id          UInt64 CODEC(ZSTD(1)),
    bin         String CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree
order by (id,bin)
primary key id
partition by id % 32
;
-- get the schema
drop table if exists btc.transactions_null;
create table btc.transactions_null engine = Null empty as select * from s3('s3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-13/part-00000-*.snappy.parquet','NOSIGN', 'Parquet'
) settings schema_inference_make_columns_nullable=1;

-- s3queue
drop table if exists btc.transactions_queue;
truncate table btc.transactions;
truncate table btc.scripts;
truncate table btc.wallets;
truncate table btc.witnesses;
create table if not exists btc.transactions_queue
ENGINE = S3Queue('s3://aws-public-blockchain/v1.0/btc/transactions/date=*/part-00000-*.parquet', 'NOSIGN', 'Parquet')
SETTINGS mode = 'ordered',
            keeper_path = '/s3queue/blockchain/v1.0/btc',
            date_time_input_format='best_effort',
            input_format_force_null_for_omitted_fields=1,
            schema_inference_make_columns_nullable=1,
            polling_min_timeout_ms = 600000,
            polling_max_timeout_ms = 6000000,
            max_processed_files_before_commit=1,
            processing_threads_num = 2,
            loading_retries = 60
as btc.transactions_null
;

create or replace function tENNS as (t,i) -> ifNull(tupleElement(t,i),'');
create or replace function tENNN as (t,i) -> ifNull(tupleElement(t,i),0);
create or replace function hashID as (s) -> if(s = '', 0, cityHash64(s));

drop view if exists btc.transactions_mv;
create materialized view btc.transactions_mv to btc.transactions as
-- todo: remove workaround for bug with tuple names in MVs - https://github.com/ClickHouse/ClickHouse/issues/52121
with  arrayMap(x->(
                   hashID(tENNS(x,'address')),
                   tENNN(x,'index'),
                   tENNN(x,'required_signatures'),
                   hashID(tENNS(x,'script_hex')),
                   tENNN(x,'sequence'),
                   tENNN(x,'spent_output_index'),
                   reinterpretAsUInt256(unhex(tENNS(x,'spent_transaction_hash'))),
                   arrayMap(y->hashID(unhex(y)),tENNS(x,'txinwitness')),
                   tENNS(x,'type'),
                   tENNN(x,'value')
                ), inputs) as new_inp,
      arrayMap(x->(
                   hashID(tENNS(x,'address')),
                   hashID(tENNS(x,'script_hex')),
                   tENNN(x,'required_signatures')
                ), outputs) as new_outp
select
       reinterpretAsUInt256(unhex(ifNull(hash,'')))       as hash,
       ifNull(version,0)            as version,
       ifNull(size,0)               as size,
       reinterpretAsUInt256(unhex(ifNull(block_hash,''))) as block_hash,
       ifNull(block_number,0)       as block_number,
       ifNull(`index`,0)            as `index`,
       ifNull(virtual_size,0)       as virtual_size,
       ifNull(lock_time,0)          as lock_time,
       ifNull(input_count,0)        as input_count,
       ifNull(output_count,0)       as output_count,
       ifNull(is_coinbase,0)        as is_coinbase,

       ifNull(output_value,0)               as output_value,
       new_outp.1                           as `outputs.address`,
       tupleElement(outputs,'index')        as `outputs.index`,
       new_outp.3                           as `outputs.required_signatures`,
       new_outp.2                           as `outputs.script_id`,
       tupleElement(outputs,'type')         as `outputs.type`,
       tupleElement(outputs,'value')        as `outputs.value`,
       ifNull(block_timestamp,toDateTime(0)) as block_timestamp,
       --date,
       ifNull(last_modified,toDateTime(0))  as last_modified,
       ifNull(fee,0)                        as fee,

       ifNull(input_value,0)                as input_value,
       new_inp.1                            as `inputs.address`,
       new_inp.2                            as `inputs.index`,
       new_inp.3                            as `inputs.required_signatures`,
       new_inp.4                            as `inputs.script_id`,
       new_inp.5                            as `inputs.sequence`,
       new_inp.6                            as `inputs.spent_output_index`,
       new_inp.7                            as `inputs.spent_transaction_hash`,
       new_inp.8                            as `inputs.txinwitness`,
       new_inp.9                            as `inputs.type`,
       new_inp.10                           as `inputs.value`
from btc.transactions_null;
;
drop view if exists btc.wallets_mv ;
create materialized view btc.wallets_mv to btc.wallets as
with arrayJoin(arrayConcat(
        arrayMap(x->(tENNS(x,'address'),tENNN(x,'value'),'i'),inputs),
        arrayMap(x->(tENNS(x,'address'),tENNN(x,'value'),'o'),outputs)
     )) as w
select hashID(w.1) as id,
       if(w.3='i',w.2,0) as input_value,
       if(w.3='i',1,0) as input_count,
       if(w.3='o',w.2,0) as ouput_value,
       if(w.3='o',1,0) as ouput_count,
       [w.1] as orig
from btc.transactions_null
;
drop view if exists btc.witnesses_mv ;
create materialized view btc.witnesses_mv to btc.witnesses as
with arrayJoin(arrayFlatten(tENNS(inputs,'txinwitness'))) as w
select hashID(w) id, unhex(w) as bin
from btc.transactions_null
;
insert into btc.transactions_null
select * from s3('s3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-01/part-00000-*.snappy.parquet', 'NOSIGN', 'Parquet')
limit 10;
;
drop view if exists btc.scripts_mv;
create materialized view btc.scripts_mv to btc.scripts as
with arrayJoin(arrayConcat(
                    arrayMap(x->(tENNS(x,'script_hex')),inputs),
                    arrayMap(x->(tENNS(x,'script_hex')),outputs)
                )) as w
select hashID(w) as id, unhex(w) as bin
from btc.transactions_null
;
-- need for ability to disable consuming in one operation and on server start
drop view if exists btc.transactions_null_mv;
create materialized view btc.transactions_null_mv to btc.transactions_null as
select * from btc.transactions_queue
where not throwIf(if(uptime() < 300, sleep(1)+sleep(2)+sleep(3), 1) = 0,'waiting '||toString(300 - uptime()) || 'seconds to give all MVs chance to start')
;
