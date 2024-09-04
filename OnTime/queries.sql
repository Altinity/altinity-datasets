-- example queries (originally from https://clickhouse.com/docs/en/getting-started/example-datasets/ontime#queries)
-- Q0.
SELECT avg(c1)
FROM
(
    SELECT Year, Month, count(*) AS c1
    FROM ontime
    GROUP BY Year, Month
);

--Q1. The number of flights per day from the year 2000 to 2008
SELECT DayOfWeek, count(*) AS c
FROM ontime
--WHERE Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;

--Q2. The number of flights delayed by more than 10 minutes, grouped by the day of the week, for 2000-2008
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;

--Q3. The number of delays by the airport for 2000-2008
SELECT Origin, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY Origin
ORDER BY c DESC
LIMIT 10;

--Q4. The number of delays by carrier for 2007
SELECT IATA_CODE_Reporting_Airline AS Carrier, count(*)
FROM ontime
WHERE DepDelay>10 AND Year=2007
GROUP BY Carrier
ORDER BY count(*) DESC;

--Q5. The percentage of delays by carrier for 2007

SELECT Carrier, c, c2, c*100/c2 as c3
FROM
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c
    FROM ontime
    WHERE DepDelay>10
        AND Year=2007
    GROUP BY Carrier
) q
JOIN
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c2
    FROM ontime
    WHERE Year=2007
    GROUP BY Carrier
) qq USING Carrier
ORDER BY c3 DESC;

--Better version of the same query:
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
--WHERE Year=2007
GROUP BY Carrier
ORDER BY c3 DESC;

--Q6. The previous request for a broader range of years, 2000-2008
SELECT Carrier, c, c2, c*100/c2 as c3
FROM
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c
    FROM ontime
    WHERE DepDelay>10
        AND Year>=2000 AND Year<=2008
    GROUP BY Carrier
) q
JOIN
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c2
    FROM ontime
    WHERE Year>=2000 AND Year<=2008
    GROUP BY Carrier
) qq USING Carrier
ORDER BY c3 DESC;

--Better version of the same query:
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
WHERE Year>=2000 AND Year<=2008
GROUP BY Carrier
ORDER BY c3 DESC;

--Q7. Percentage of flights delayed for more than 10 minutes, by year
SELECT Year, c1/c2
FROM
(
    select
        Year,
        count(*)*100 as c1
    from ontime
    WHERE DepDelay>10
    GROUP BY Year
) q
JOIN
(
    select
        Year,
        count(*) as c2
    from ontime
    GROUP BY Year
) qq USING (Year)
ORDER BY Year;

--Better version of the same query:
SELECT Year, avg(DepDelay>10)*100
FROM ontime
GROUP BY Year
ORDER BY Year;

--Q8. The most popular destinations by the number of directly connected cities for various year ranges
SELECT DestCityName, uniqExact(OriginCityName) AS u
FROM ontime
WHERE Year >= 2000 and Year <= 2010
GROUP BY DestCityName
ORDER BY u DESC LIMIT 10;

--Q9.
SELECT Year, count(*) AS c1
FROM ontime
GROUP BY Year;

--Q10.
SELECT
   min(Year), max(Year), IATA_CODE_Reporting_Airline AS Carrier, count(*) AS cnt,
   sum(ArrDelayMinutes>30) AS flights_delayed,
   round(sum(ArrDelayMinutes>30)/count(*),2) AS rate
FROM ontime
WHERE DayOfWeek NOT IN (6,7)
   AND OriginState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND DestState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND FlightDate < '2010-01-01'
GROUP by Carrier
HAVING cnt>100000 and max(Year)>1990
ORDER by rate DESC
LIMIT 1000;

--Bonus:
SELECT avg(cnt)
FROM
(
    SELECT Year,Month,count(*) AS cnt
    FROM ontime
    WHERE DepDel15=1
    GROUP BY Year,Month
);

SELECT avg(c1) FROM
(
    SELECT Year,Month,count(*) AS c1
    FROM ontime
    GROUP BY Year,Month
);

SELECT DestCityName, uniqExact(OriginCityName) AS u
FROM ontime
GROUP BY DestCityName
ORDER BY u DESC
LIMIT 10;

SELECT OriginCityName, DestCityName, count() AS c
FROM ontime
GROUP BY OriginCityName, DestCityName
ORDER BY c DESC
LIMIT 10;

SELECT OriginCityName, count() AS c
FROM ontime
GROUP BY OriginCityName
ORDER BY c DESC
LIMIT 10;