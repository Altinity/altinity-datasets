#!/bin/bash

#BASKET="https://test-altinity-support-team.s3.amazonaws.com/On_Time_Reporting_Carrier_On_Time_Performance_1987_present/"
BASKET="https://altinity-clickhouse-data.s3.amazonaws.com/airline/data/ontime_parquet2/"

TEMP_DIR=$(mktemp -d)
SOURCE="https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"

for y in `seq 2024 -1 1987` ; do
   for m in `seq 1 12` ; do
     MONTH=$(printf "%02d" $m)
     OUTFILE="${y}${MONTH}.parquet"
     #OUTFILE="year=${y}/month=${m}/${y}${MONTH}.parquet"

     clickhouse-local -q "select count() from s3('${BASKET}/${OUTFILE}')" > /dev/null 2>&1

     if [ $? -eq 0 ]; then
       echo "File ${OUTFILE} already exists on S3."
       continue
     else
       echo "Loading ${SOURCE}_${y}_${m}.zip ...."
     fi

     wget --no-check-certificate ${SOURCE}_${y}_${m}.zip -O $TEMP_DIR/data.zip > /dev/null 2>&1

     if [ $? -ne 0 ]; then
       echo "Failed to download ${SOURCE}_${y}_${m}.zip."
       continue
     fi

     unzip -o $TEMP_DIR/data.zip -d $TEMP_DIR

     if [ $? -ne 0 ]; then
       echo "Failed to unzip ${TEMP_DIR}/data.zip."
       continue
     fi

     CSV_FILE=$(find $TEMP_DIR -name "*.csv")

     if [ -z "$CSV_FILE" ]; then
       echo "No CSV file found after unzipping."
       continue
     fi

     clickhouse-local -q "insert into function s3('${BASKET}/${OUTFILE}') select * from file('${CSV_FILE}','CSVWithNames')"
     if [ $? -ne 0 ]; then
       echo "Failed to upload to ${OUTFILE}"
     else
       echo "Uploaded to ${OUTFILE}"
     fi

     rm -rf $TEMP_DIR/*
   done
done

rm -rf $TEMP_DIR
