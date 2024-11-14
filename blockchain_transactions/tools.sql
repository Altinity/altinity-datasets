select distinct _time,_path
from s3('s3://aws-public-blockchain/v1.0/btc/transactions/date={2009..2023}-*/part-00000-*.parquet', 'NOSIGN', 'Parquet')
order by _path;
select min(_path) from s3('s3://aws-public-blockchain/v1.0/btc/transactions/date=2009-*/part-00000-*.parquet', 'NOSIGN', 'Parquet');

select formatReadableSize(unreserved_space) from system.disks;

select * from system.text_log
where event_time > now() - interval 10 minute
  and logger_name not in ['RaftInstance','KeeperTCPHandler','DNSCacheUpdater']
order by event_time desc
;
select view_name,event_time,view_duration_ms d,written_rows w,read_rows r,exception
from system.query_views_log
where event_time > now()-interval 1 day
order by event_time desc
;
select event_time, table, splitByChar('/',file_name)[4] as f, date_diff(second, processing_start_time, processing_end_time) as d, exception
from system.s3queue_log
where event_time > '2024-11-14 12:00:00' --now() - interval 10 hour
  --and file_name='v1.0/btc/transactions/date=2024-09-20/part-00000-e0c3ae9f-012f-4f89-b5f0-907a36944224-c000.snappy.parquet'
  --and exception != ''
order by event_time desc
;

select 'btc_transactions',formatReadableQuantity(count()) from btc_transactions
 union all
select 'btc_wallets',formatReadableQuantity(count()) from btc_wallets
 union all
select 'btc_scripts',formatReadableQuantity(count()) from btc_scripts
;
--top balances
select orig, input_value as i, output_value as o, i-o as d
from btc_wallets final
order by d desc
limit 100
;
