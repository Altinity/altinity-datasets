
path - s3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-01/
here are day files:
   - part-00000-674d83e4-f858-4fdc-adbd-cacd57175a28-c000.snappy.parquet 
and during the day incremental data in files like:
   - 869917.snappy.parquet

```
SELECT DISTINCT _time, _path
FROM s3('s3://aws-public-blockchain/v1.0/btc/transactions/date=2024-11-**.snappy.parquet', 'NOSIGN', 'Parquet')
ORDER BY _path ASC;
```

