# Airline Data

Cleaned up airline ontime data with airports table to illustrate joins

## Hive path formatting

Allow to run queries with filtering by virtual columns inferred from the list of objects in the basket, without reading actual data files.
```
SET use_hive_partitioning = 1;
select count() from s3('s3://altinity-clickhouse-data/airline/data/ontime_parquet3/year=*/month=*/*.parquet')
where year=2000 and month=1;
```

## Load data to local disk

If you want to make some experiments, you can create a local MergeTree table and load data from provided parquet file. It takes 5Gb.
See [ontime_schema.sql](ontime_schema.sql)file table_schema.sql for schema example.

Load instructions:
```
insert into ontime select *
    except (Year, Quarter, Month, DayofMonth, DayOfWeek), -- remove non-needed columns from source
    Div2Airport, Div2AirportID    -- add columns for experiments
from s3('s3://altinity-clickhouse-data/airline/data/ontime_parquet3/year=*/*/*.parquet')
-- increase insert batch size to reduce amount of created parts
settings min_insert_block_size_bytes = 2147483648, --2Gb, uncompressed!  Can be half of available instance RAM
          min_insert_block_size_rows = 33554432, --32M rows  May set as 1B
               max_insert_block_size = 33554432 --32M rows
              -- ,max_insert_threads=20 -- will read faster if there are enough CPU cores
              -- ,max_threads=20   -- will create more parts
;
```
more RAM you have on the loading server, bigger batch you can create and fewer parts will be created.

Force merging all parts to 1 (per partition). Will read all data and write it back to disk.
```
optimize table ontime final; 
```

## s3_plain_rewritable

It's possible to "mount" MergeTree table in RO mode directly from s3.

see [s3_plain_rewritable.sql](s3_plain_rewritable.sql) for schema and UUID. 

you can use clickhouse-local to mount table locally and run [test queries](queries.sql)

```
clickhouse-local --interactive --queries-file s3_plain_rewritable.sql
```
It takes some time, to load important data (like PK columns) from S3.

Check state in system.tables:
```
select * from system.tables where table='ontime'\G

database:                      t1
name:                          ontime
uuid:                          b86a87e9-e3c6-426d-a26b-1f62de8bf6a4
engine:                        MergeTree
is_temporary:                  0
data_paths:                    ['airline/data/ontime_plain_rewritable/store/b86/b86a87e9-e3c6-426d-a26b-1f62de8bf6a4/']
metadata_path:                 /tmp/clickhouse-local-b2f29dce-3f76-4387-9360-4ffab9197236/store/ac6/ac6491d7-fe68-472c-83a9-5f46e18b7be9/ontime.sql
metadata_modification_time:    2024-09-04 11:12:35
metadata_version:              0
dependencies_database:         []
dependencies_table:            []
create_table_query:            CREATE TABLE t1.ontime ...
engine_full:                   MergeTree ORDER BY FlightDate SETTINGS disk = disk(type = 's3_plain_rewritable', ...
as_select:
partition_key:
sorting_key:                   FlightDate
primary_key:                   FlightDate
sampling_key:
storage_policy:                ____tmp_internal_83985161937052733602718121444335415201
total_rows:                    220270632
total_bytes:                   16082132856
total_bytes_uncompressed:      36743977538
parts:                         1
active_parts:                  1
total_marks:                   26894
```

system.parts:
```
:) select * from system.parts where table='ontime' \G;

partition:                             tuple()
name:                                  all_1_10_2
uuid:                                  00000000-0000-0000-0000-000000000000
part_type:                             Wide
active:                                1
marks:                                 26894
rows:                                  220270632
bytes_on_disk:                         16082132856
data_compressed_bytes:                 16076959900
data_uncompressed_bytes:               36631614406
primary_key_size:                      50445
marks_bytes:                           5115401
secondary_indices_compressed_bytes:    0
secondary_indices_uncompressed_bytes:  0
secondary_indices_marks_bytes:         0
modification_time:                     2024-09-04 11:12:34
remove_time:                           1970-01-01 00:00:00
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
min_time:                              1970-01-01 00:00:00
max_time:                              1970-01-01 00:00:00
partition_id:                          all
min_block_number:                      1
max_block_number:                      10
level:                                 2
data_version:                          1
primary_key_bytes_in_memory:           0
primary_key_bytes_in_memory_allocated: 0
is_frozen:                             0
database:                              t1
table:                                 ontime
engine:                                MergeTree
disk_name:                             __tmp_internal_83985161937052733602718121444335415201
path:                                  airline/data/ontime_plain_rewritable/store/b86/b86a87e9-e3c6-426d-a26b-1f62de8bf6a4/all_1_10_2/
hash_of_all_files:                     72643b929e73b2fd3a541952b0580ae5
hash_of_uncompressed_files:            74db2c54fbf94572fb538f0f6f4fe7be
uncompressed_hash_of_compressed_files: 12e116851a2ece2c4cab2e288221c790
```

Run some query:
```
:) SELECT avg(c1) FROM (
    SELECT Year, Month, count(*) AS c1
    FROM ontime
    GROUP BY Year, Month
);

   ┌───────────avg(c1)─┐
1. │ 499479.8911564626 │
   └───────────────────┘

1 row in set. Elapsed: 2.953 sec. Processed 215.64 million rows, 431.27 MB (73.03 million rows/s., 146.07 MB/s.)
Peak memory usage: 1.97 MiB.
```
