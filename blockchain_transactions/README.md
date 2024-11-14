
path - s3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-01/
here are placed day files:
   - part-00000-674d83e4-f858-4fdc-adbd-cacd57175a28-c000.snappy.parquet 
and during the day incremental data in files like:
   - 869917.snappy.parquet

```
SELECT DISTINCT _time, _path
FROM s3('s3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-**.snappy.parquet', 'NOSIGN', 'Parquet')
ORDER BY _path ASC;
```

### params for btc_transactions

- Daily file contains 600-700k rows that need to be sorted. Take care of RAM when loading data on small instances.
(processing_threads_num = 1 could help, 2 is also OK for 14Gb RAM instance).
- Some timestamps can be "not ideal", better switch default to best_effort. 
(date_time_input_format='best_effort')

### s3queue params in general
- s3queue_settings shows wrong info in 24.10
- s3 path is stored in table engine (sql file on disk, visible in system.tables)
- zk path is not cleared on table drop, can be reused on same or different node
- alter table modify settings does not work, you can change settings by editing ZK data manually in detached state


### Tables

Need to denormalize schema to store less data for wallet addresses and script/asm code

- btc_transactions - main table
- btc_wallets - ordered by UInt64, aggmt, two sums and two counts, array of orig address
- btc_script - rockdb, pk by hash of hex, columns: hex, code

### Array(Tuple()) problem

schema inference creates Array(Tuple()), but we need Nested.
so we have to use indexes like output.1 in transformations
