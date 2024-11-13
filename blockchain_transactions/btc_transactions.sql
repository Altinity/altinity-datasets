
-- get the schema
create table default.btc_transactions engine = MergeTree order by tuple() empty as
select * from s3(
        's3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-12/part-00000-*.snappy.parquet',
        'NOSIGN', 'Parquet'
) settings schema_inference_make_columns_nullable=1;

show create table default.btc_transactions;
-- create table. remove Nullable for date and block_hash columns
--drop table default.btc_transactions;
CREATE TABLE default.btc_transactions
(
    `hash` String,
    `version` Nullable(Int64),
    `size` Nullable(Int64),
    `block_hash` String,
    `block_number` Nullable(Int64),
    `index` Nullable(Int64),
    `virtual_size` Nullable(Int64),
    `lock_time` Nullable(Int64),
    `input_count` Nullable(Int64),
    `output_count` Nullable(Int64),
    `is_coinbase` Nullable(Bool),
    `output_value` Nullable(Float64),
    `outputs` Array(Tuple(
        address Nullable(String),
        index Nullable(Int64),
        required_signatures Nullable(Int64),
        script_asm Nullable(String),
        script_hex Nullable(String),
        type Nullable(String),
        value Nullable(Float64))),
    `block_timestamp` Nullable(DateTime64(9)),
    `date` Date,
    `last_modified` Nullable(DateTime64(9)),
    `fee` Nullable(Float64),
    `input_value` Nullable(Float64),
    `inputs` Array(Tuple(
        address Nullable(String),
        index Nullable(Int64),
        required_signatures Nullable(Int64),
        script_asm Nullable(String),
        script_hex Nullable(String),
        sequence Nullable(Int64),
        spent_output_index Nullable(Int64),
        spent_transaction_hash Nullable(String),
        txinwitness Array(Nullable(String)),
        type Nullable(String),
        value Nullable(Float64)))
)
ENGINE = MergeTree
partition by toYYYYMM(date)
order by (date,block_hash,hash)
settings non_replicated_deduplication_window=1000
;
-- s3queue stuff
--drop table default.btc_transactions_queue;
create table default.btc_transactions_queue
(
    `hash` Nullable(String),
    `version` Nullable(Int64),
    `size` Nullable(Int64),
    `block_hash` String,
    `block_number` Nullable(Int64),
    `index` Nullable(Int64),
    `virtual_size` Nullable(Int64),
    `lock_time` Nullable(Int64),
    `input_count` Nullable(Int64),
    `output_count` Nullable(Int64),
    `is_coinbase` Nullable(Bool),
    `output_value` Nullable(Float64),
    `outputs` Array(Tuple(
        address Nullable(String),
        index Nullable(Int64),
        required_signatures Nullable(Int64),
        script_asm Nullable(String),
        script_hex Nullable(String),
        type Nullable(String),
        value Nullable(Float64))),
    `block_timestamp` Nullable(DateTime64(9)),
    `date` Date,
    `last_modified` Nullable(DateTime64(9)),
    `fee` Nullable(Float64),
    `input_value` Nullable(Float64),
    `inputs` Array(Tuple(
        address Nullable(String),
        index Nullable(Int64),
        required_signatures Nullable(Int64),
        script_asm Nullable(String),
        script_hex Nullable(String),
        sequence Nullable(Int64),
        spent_output_index Nullable(Int64),
        spent_transaction_hash Nullable(String),
        txinwitness Array(Nullable(String)),
        type Nullable(String),
        value Nullable(Float64)))
)
ENGINE = S3Queue('s3://aws-public-blockchain/v1.0/btc/transactions/date=2024-*/part-00000-*.parquet', 'NOSIGN', 'Parquet')
SETTINGS mode = 'ordered',
            keeper_path = '/s3queue/blockchain/v1.0/btc',
            date_time_input_format='best_effort',
            input_format_force_null_for_omitted_fields=1,
            polling_min_timeout_ms = 300000,
            polling_max_timeout_ms = 600000,
            max_processed_files_before_commit=1,
            processing_threads_num = 1,
            loading_retries = 10
;
--drop view default.btc_transactions_mv;
CREATE MATERIALIZED VIEW default.btc_transactions_mv TO default.btc_transactions AS
SELECT * FROM default.btc_transactions_queue
;

