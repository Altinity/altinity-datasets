-- from here https://gist.github.com/filimonov/f56ead8ba91d525b99654770fa56f57c
SELECT
    min(date),
    max(date),
    count(),
    uniq(userid),
    uniq(sessionid)
FROM hits_retail
;
SELECT
    path1,
    count()
FROM hits_retail
GROUP BY path1
;
SELECT ref_domain, count() FROM hits_retail  GROUP BY ref_domain order by count() desc limit 30;

SELECT
    ref_domain,
    count()
FROM hits_retail
GROUP BY ref_domain
ORDER BY count() DESC
LIMIT 30;

SELECT
    device_type,
    count()
FROM hits_retail
GROUP BY device_type
;

SELECT
    products.category,
    min(products.price),
    max(products.price),
    count()
FROM hits_retail
ARRAY JOIN products
WHERE path1 = 'purchase'
GROUP BY products.category
ORDER BY count() DESC
LIMIT 20;

-- add ‘context’ data to each event in session
SELECT
    event_data.1 AS timestamp,
    sessionid,
    eventnumber,
    reversed_eventnumber,
    orderid,
    events_count,
    event_data.2 AS path1,
    event_data.3 AS path2,
    if(time_on_page_raw < 0, NULL, time_on_page_raw) AS time_on_page
FROM
(
    WITH
        groupArray((timestamp, path1, path2)) AS event_data_all,
        arrayEnumerate(event_data_all) AS eventnumber_all
    SELECT
        sessionid,
        eventnumber_all,
        count() AS events_count,
        arraySort(x -> x.1, event_data_all) AS sorted_event_data_all,
        reverse(eventnumber_all) AS reversed_eventnumber_all,
        anyIf(hex(timestamp), path1 = 'purchase') AS orderid,
        arrayMap(i -> (sorted_event_data_all[(i + 1)].1 - sorted_event_data_all[i].1), eventnumber_all) AS time_on_page_all
    FROM hits_retail
    WHERE (date >= '2017-01-01') AND (date <= '2017-01-01')
    GROUP BY sessionid
)
ARRAY JOIN
    sorted_event_data_all AS event_data,
    eventnumber_all AS eventnumber,
    reversed_eventnumber_all AS reversed_eventnumber,
    time_on_page_all AS time_on_page_raw
ORDER BY
    sessionid ASC,
    eventnumber ASC
LIMIT 30;

--Find first/last event per group.

SELECT
    sessionid,
    argMin(concat(path1, '/', path2), timestamp) AS first_url,
    argMax(concat(path1, '/', path2), timestamp) AS last_url,
    count()
FROM hits_retail
GROUP BY sessionid
LIMIT 10
;

SELECT
    last_url,
    count() AS session_count
FROM
(
    SELECT
        sessionid,
        argMin(concat(path1, '/', path2), timestamp) AS first_url,
        argMax(concat(path1, '/', path2), timestamp) AS last_url,
        count()
    FROM hits_retail
    GROUP BY sessionid
)
GROUP BY last_url
ORDER BY session_count DESC
LIMIT 10;

SELECT maxIntersections(toUInt32(start), toUInt32(end))
FROM
(
    SELECT
        sessionid,
        min(timestamp) AS start,
        max(timestamp) AS end
    FROM hits_retail
    GROUP BY sessionid
)
;
SELECT
    toStartOfHour(start) AS hour,
    maxIntersections(toUInt32(start), toUInt32(end)) AS max_simultaneous_sessions,
    bar(max_simultaneous_sessions, 10, 900)
FROM
(
    SELECT
        sessionid,
        min(timestamp) AS start,
        max(timestamp) AS end
    FROM hits_retail
    WHERE (date >= '2017-01-01') AND (date <= '2017-01-03')
    GROUP BY sessionid
)
GROUP BY hour
ORDER BY hour ASC
;

SELECT
    min(start),
    max(end),
    toDateTime(maxIntersectionsPosition(toUInt32(start), toUInt32(end)))
FROM
(
    SELECT
        sessionid,
        min(timestamp) AS start,
        max(timestamp) AS end
    FROM hits_retail
    GROUP BY sessionid
)
;

SELECT
    sessionid,
    minIf(timestamp, path1 = 'checkout') AS checkout_begin_time,
    minIf(timestamp, path1 = 'purchase') AS purchase_time
FROM hits_retail
WHERE (path1 = 'checkout') OR (path1 = 'purchase')
GROUP BY sessionid
LIMIT 10
;

WITH toDate('2017-01-01') AS last_date
SELECT
    uniqIf(userid, date = last_date) AS users_today,
    uniqIf(userid, date >= (last_date - 7)) AS users_last_week,
    uniqIf(userid, date >= (last_date - 30)) AS users_last_month
FROM hits_retail
WHERE date >= (last_date - 30);

SELECT quantiles(0.05, 0.2, 0.5, 0.95)(checkout_duration)
FROM
(
    SELECT
        sessionid,
        minIf(timestamp, path1 = 'checkout') AS checkout_begin_time,
        minIf(timestamp, path1 = 'purchase') AS purchase_time,
        dateDiff('second', checkout_begin_time, purchase_time) AS checkout_duration
    FROM hits_retail
    WHERE (path1 = 'checkout') OR (path1 = 'purchase')
    GROUP BY sessionid
    HAVING (purchase_time != 0) AND (checkout_begin_time != 0)
)
;


SELECT
    userid,
    sessionid
FROM hits_retail
GROUP BY
    userid,
    sessionid
HAVING (minIf(timestamp, path1 = 'purchase') > 0) AND (minIf(timestamp, path1 = 'purchase') < minIf(timestamp, path1 = 'checkout'))
;

SELECT
    sessionid,
    timestamp,
    concat(path1, '/', path2)
FROM hits_retail
PREWHERE (userid, sessionid) IN
(
    SELECT
        userid,
        sessionid
    FROM hits_retail
    GROUP BY
        userid,
        sessionid
    HAVING (minIf(timestamp, path1 = 'purchase') > 0) AND (minIf(timestamp, path1 = 'purchase') < minIf(timestamp, path1 = 'checkout'))
    LIMIT 1
)
ORDER BY
    sessionid ASC,
    timestamp ASC
;

SELECT
    userid,
    sessionid
FROM hits_retail
GROUP BY
    userid,
    sessionid
HAVING sequenceMatch('(?1).*(?2).*(?1).*(?2).*(?3)')(timestamp, path1 = 'product', path1 = 'checkout', path1 = 'purchase')
LIMIT 2
;

SELECT
    sessionid,
    timestamp,
    concat(path1, '/', path2)
FROM hits_retail
PREWHERE (userid, sessionid) IN
(
    SELECT
        userid,
        sessionid
    FROM hits_retail
    GROUP BY
        userid,
        sessionid
    HAVING sequenceMatch('(?1).*(?2).*(?1).*(?2).*(?3)')(timestamp, path1 = 'product', path1 = 'checkout', path1 = 'purchase') AND (count() < 20)
    LIMIT 1
)
ORDER BY
    sessionid ASC,
    timestamp ASC
;

SELECT quantiles(0.25, 0.5, 0.75)(cat2prod)
FROM
(
    SELECT sequenceCount('(?1)(?2)')(timestamp, path1 = 'category', path1 = 'product') AS cat2prod
    FROM hits_retail
    GROUP BY userid
    HAVING countIf(path1 = 'purchase') > 0
)
;


SELECT quantiles(0.25, 0.5, 0.75)(cat2prod)
FROM
(
    SELECT sequenceCount('(?1)(?2)')(timestamp, path1 = 'category', path1 = 'product') AS cat2prod
    FROM hits_retail
    GROUP BY userid
    HAVING countIf(path1 = 'purchase') = 0
)
;

SELECT
    count() AS users_overall,
    countIf(funnel_depth >= 0) AS users_at_funnel_step0,
    countIf(funnel_depth >= 1) AS users_at_funnel_step1,
    countIf(funnel_depth >= 2) AS users_at_funnel_step2,
    countIf(funnel_depth >= 3) AS users_at_funnel_step3,
    countIf(funnel_depth = 4) AS users_at_funnel_step4
FROM
(
    SELECT
        userid,
        windowFunnel(300)(timestamp, path1 = 'product', path1 = 'basket', path1 = 'checkout', path1 = 'purchase') AS funnel_depth
    FROM hits_retail
    GROUP BY userid
);


--"FAST CLICKER" :)

SELECT
    userid,
    sessionid,
    timestamp,
    concat(path1, '/', path2)
FROM hits_retail
PREWHERE (userid, sessionid) IN
(
    SELECT
        userid,
        sessionid
    FROM hits_retail
    GROUP BY
        userid,
        sessionid
    HAVING windowFunnel(10)(timestamp, path1 = 'product', path1 = 'basket', path1 = 'checkout', path1 = 'purchase') = 4
    LIMIT 1
)
ORDER BY
    userid ASC,
    sessionid ASC,
    timestamp ASC
;

SELECT
    userid,
    retention(date = '2017-01-01', date = '2017-01-02', date = '2017-01-03', date = '2017-01-04') AS ret
FROM hits_retail
WHERE date BETWEEN '2017-01-01' and  '2017-01-04'
GROUP BY userid
HAVING countIf( date = '2017-01-01' ) > 0
LIMIT 5
;


SELECT sumForEach(ret)
FROM
(
    SELECT
        userid,
        retention(date = '2017-01-01', date = '2017-01-02', date = '2017-01-03', date = '2017-01-04', date = '2017-01-05', date = '2017-01-06', date = '2017-01-07', date = '2017-01-08') AS ret
    FROM hits_retail
    WHERE (date >= '2017-01-01') AND (date <= '2017-01-08')
    GROUP BY userid
)
;

WITH toDate('2017-01-01') AS starting_date
SELECT
    userid,
    retention(date = starting_date, date = (starting_date + 1), date = (starting_date + 2), date = (starting_date + 3), date = (starting_date + 4), date = (starting_date + 5), date = (starting_date + 6), date = (starting_date + 7)) AS ret
FROM hits_retail
WHERE (date >= starting_date) AND (date <= (starting_date + 10))
GROUP BY userid
LIMIT 30
;



