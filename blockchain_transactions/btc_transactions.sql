-- get the schema
drop table btc_transactions_null;
create table btc_transactions_null engine = Null empty as
select * from s3(
        's3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-12/part-00000-*.snappy.parquet',
        'NOSIGN', 'Parquet'
) settings schema_inference_make_columns_nullable=0;

-- s3queue
drop table btc_transactions_queue;
truncate table btc_transactions;
truncate table btc_scripts;
truncate table btc_wallets;
create table btc_transactions_queue
ENGINE = S3Queue('s3://aws-public-blockchain/v1.0/btc/transactions/date={2020..2030}-*/part-00000-*.parquet', 'NOSIGN', 'Parquet')
SETTINGS mode = 'ordered',
            keeper_path = '/s3queue/blockchain/v1.0/btc',
            date_time_input_format='best_effort',
            input_format_force_null_for_omitted_fields=0,
            schema_inference_make_columns_nullable=0,
            polling_min_timeout_ms = 600000,
            polling_max_timeout_ms = 6000000,
            max_processed_files_before_commit=1,
            processing_threads_num = 2,
            loading_retries = 10
as btc_transactions_null
;

show create table btc_transactions_queue;

-- need for ability to disable consuming in one operation and on server start
drop view btc_transactions_null_mv;
create materialized view btc_transactions_null_mv to btc_transactions_null as
select * from btc_transactions_queue
where not throwIf(if(uptime() < 300, sleep(1)+sleep(2)+sleep(3), 1) = 0,'waiting all MVs start')
;
--drop table btc_transactions;
CREATE TABLE btc_transactions
(
    hash                      FixedString(32) CODEC(ZSTD(1)),   -- unhex
    version                   UInt8           CODEC(ZSTD(1)),
    size                      UInt32          CODEC(ZSTD(1)),
    block_hash                FixedString(32) CODEC(ZSTD(1)),   -- unhex
    block_number              UInt32          CODEC(ZSTD(1)),
    `index`                   UInt16          CODEC(ZSTD(1)),
    virtual_size              UInt32          CODEC(ZSTD(1)),
    lock_time                 UInt64          CODEC(ZSTD(1)),
    input_count               UInt16          CODEC(ZSTD(1)),
    output_count              UInt16          CODEC(ZSTD(1)),
    is_coinbase               Bool,
    output_value              Float64         CODEC(ZSTD(1)),
    outputs Nested (
        address               UInt64,
        `index`               UInt16,
        required_signatures   UInt8,
        script_id             UInt64,     -- hash of script_hex
        type                  LowCardinality(String),
        value                 Float64
    )                                          CODEC(ZSTD(1)),
    block_timestamp           DateTime         CODEC(ZSTD(1)),
    date                      Date             CODEC(ZSTD(1)),
    last_modified             Nullable(DateTime64(6)) CODEC(ZSTD(1)),
    fee                       Float64          CODEC(ZSTD(1)),
    input_value               Float64          CODEC(ZSTD(1)),
    inputs Nested(
        address               UInt64,
        `index`               UInt16,
        required_signatures   UInt8,
        script_id             UInt64,             -- hash of script_hex
        sequence              UInt64,
        spent_output_index    UInt16,
        spent_transaction_hash FixedString(32),   -- unhex
        txinwitness           Array(String),      -- unhex
        type                  LowCardinality(String),
        value                 Float64
    )                                           CODEC(ZSTD(1))
)
ENGINE = MergeTree
partition by toYYYYMM(date)
order by (date,block_hash,hash)
settings non_replicated_deduplication_window=1000
;
drop view btc_transactions_mv;
create materialized view btc_transactions_mv to btc_transactions as
select
       unhex(hash) as hash,
       version,
       size,
       unhex(block_hash) as block_hash,
       block_number,`index`,virtual_size,lock_time,
       input_count, output_count,
       is_coinbase,

       output_value,
       arrayMap(x->cityHash64(x),outputs.1) as `outputs.address`,
       outputs.2 as `outputs.index`,
       outputs.3 as `outputs.required_signatures`,
       arrayMap(x->cityHash64(x),outputs.4) as `outputs.script_id`,
       outputs.6 as `outputs.type`,
       outputs.7 as `outputs.value`,
       block_timestamp, date, last_modified,
       fee,

       input_value,
       arrayMap(x->cityHash64(x),inputs.1) as `inputs.address`,
       inputs.2 as `inputs.index`,
       inputs.3 as `inputs.required_signatures`,
       arrayMap(x->cityHash64(x),inputs.4) as `inputs.script_id`,
       inputs.6 as `inputs.sequence`,
       inputs.7 as `inputs.spent_output_index`,
       arrayMap(x->unhex(x),inputs.8) as `inputs.spent_transaction_hash`,
       inputs.9 as `inputs.txinwitness`,
       inputs.10 as `inputs.type`,
       inputs.11 as `inputs.value`
from btc_transactions_null
;
--drop table btc_wallets;
create table btc_wallets
(
    id UInt64,
    input_value SimpleAggregateFunction(sum,Float64),
    input_count SimpleAggregateFunction(sum,Float64),
    output_value SimpleAggregateFunction(sum,Float64),
    output_count SimpleAggregateFunction(sum,Float64),
    orig         SimpleAggregateFunction(groupUniqArrayArray,Array(String))
) ENGINE = AggregatingMergeTree
order by id
partition by id % 32
;
drop view btc_wallets_mv ;
create materialized view btc_wallets_mv to btc_wallets as
with arrayJoin(arrayConcat(arrayMap(x,y->(x,y,'i'),inputs.1,inputs.11),
       arrayMap(x,y->(x,y,'o'),outputs.1,outputs.7))) as w
select cityHash64(w.1) as id,
       if(w.3='i',w.2,0) as input_value,
       if(w.3='i',1,0) as input_count,
       if(w.3='o',w.2,0) as ouput_value,
       if(w.3='o',1,0) as ouput_count,
       [w.1] as orig
from btc_transactions_null
;
--drop table btc_scripts;
select * from btc_scripts;
create table btc_scripts
(
    id UInt64,
    hex String,
    asm String
) ENGINE = EmbeddedRocksDB
primary key id
;
drop view btc_scripts_mv;
create materialized view btc_scripts_mv to btc_scripts as
with arrayJoin(arrayConcat(arrayMap(x,y->(x,y),inputs.5,inputs.4),
       arrayMap(x,y->(x,y),outputs.5,outputs.4))) as w
select cityHash64(w.1) as id,
       w.1 as hex,
       w.2 as asm
from btc_transactions_null
;
