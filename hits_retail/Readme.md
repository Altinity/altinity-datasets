# Altinity hits_retail dataset

Then you start exploring possibilities of ClickHouse; you may want to make some trial runs of clickhouse, with a table or a few with **some** data and a simple database schema, which can be used as a reference later. So you need to take some datasets to play with.

That article presents the **hits_retail** dataset, created in Altinity for educational / demonstration purposes.

### **Why another dataset?**

We need to have an easy-to-use reference dataset for our research, articles & presentations.

One of the best options currently available is to pick one of the datasets listed in [ClickHouse docs](https://clickhouse.yandex/docs/en/getting_started/example_datasets/ontime/) or try to adopt something from [Kaggle](https://www.kaggle.com/datasets), [data.gov](https://catalog.data.gov/), or some other [awesome](https://github.com/awesomedata/awesome-public-datasets) dataset.

But there are several reasons why we need one more dataset:

1. One of the most popular usage scenarios for ClickHouse ('clickstream' data) is mostly non-covered by any dataset.
2. [Anonymized Yandex.Metrica Data](https://clickhouse.yandex/docs/en/getting_started/example_datasets/metrica/) 'hits' dataset covers that hole a bit, but it contains data mixed from several sites (not too big each), not normalized well, and due to obfuscation and Russian strings in it, it's hard to use in demos (results looks too cryptic)
3. most of the other public datasets are just too small for ClickHouse,
4. last but not least, all of the existing datasets have the same [fatal flaw](https://en.wikipedia.org/wiki/Not_invented_here) :)

So we need a dataset that should have the **following properties**:

1. 'clickstream' (recording of page hits), with sessions and typical properties like URL / referer / IP, etc.
2. well-structured, i.e., easy to distinguish different page types
3. has statistical properties as close as possible to real-life data
4. big enough for ClickHouse but small enough to be downloaded and not waste disk space.
5. has some arrays and fields of different types to demonstrate unique ClickHouse features
6. human-readable data, to make examples with results 'which have sense'.

In that article, we present the dataset we've created based on those requirements.

We called that **hits_retail**, and it simulates page hits recording on a fictional retail online store '[www.altinity.test](http://www.altinity.test/).'

### **License**

We publish that dataset under the [CC BY-NC-ND](https://creativecommons.org/licenses/by-nc-nd/3.0/) license, so you're free to use and share that for any noncommercial purpose while preserving credits to Altinity. If you need to use it for commercial purposes or distribute a modified version, please contact us (we introduce that limitation not to forbid that possibility but because we prefer to know what & why you do with that).

### **How dataset was created?**

To get realistic data features, we have created a generator script that "looks" at the data of an actual online store and generates new data while preserving all important properties.  We have no permission to name the source or original data. Of course, the publication of the original data, for obvious reasons, is impossible.

The generator script worked in the following manner: pick some session from real data according to some preferences, remap all the source values to some other values according to a set of predefined rules, and use automatically built dictionaries (so no real values were directly copied); after that remapped session was put in target data according some rules allowing to preserve all the important properties (like weekly/hourly distribution); in the meanwhile some data multiplication and filtering was also applied to adjust to the size of target dataset.

The generator script has a lot of manually defined remapping rules that relate to the original data, and due to that fact, it can't be reused for other use cases or be open-sourced.

### **Columns description**

Let's take a look at the table definition:

```sql
CREATE TABLE hits_retail
(
    `date` Date,
    `counterid` UInt8,
    `timestamp` DateTime('UTC'),
    `userid` UInt64,
    `sessionid` UInt64,
    `path1` LowCardinality(String),
    `path2` LowCardinality(String),
    `utm_medium` LowCardinality(String),
    `utm_source` LowCardinality(String),
    `ref_domain` LowCardinality(String),
    `external_userid` UInt64,
    `ip_hash` UInt32,
    `useragentid` UInt64,
    `device_type` FixedString(1),
    `products` Nested(id UInt32, price UInt32, quantity UInt16, category UInt64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (counterid, userid, sessionid, timestamp)
SAMPLE BY userid; 
```

- **date** - column contain date part of timestamp. 
- **counterid** - is not used (we have only one online store in the table) and is always 1.
- **timestamp** - the time of the page hit
- **userid** is cookie-based user identificator.
- **sessionid** is cookie-based id of user session.
- **path1** contains the the page type which was visited.
- **path2** contains subtype of the page visited. Categoryid for category pages, productid - for product pages, checkout step for checkout pages etc.
- **utm_medium** & **utm_source** - describe the campaign, i.e. if the user came to page from some advertiser - reference to that advertiser/entry will appear in utm_source & utm_megium.
- **ref_domain** - the referer domain, i.e. the domain of the page which was visited before user came to that page.
- **external_userid** - user id based on login (for registered users)
- **ip_hash** - ip of the user (not real of course, it can contain invalid ips like '0.248.99.36')
- **useragentid** - hash of user-agent string.
- **device_type** - m=mobile phone, c=computer, t=tablet
- **products** Nested - information about the products shown on that page.

### Download

```sql
INSERT INTO hits_retail SELECT *
FROM s3('https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/hits_retail3/*.parquet', 'NOSIGN')
```

Or you can directly mount the table as s3_plain:

```sql
ATTACH TABLE hits_retail UUID '060e3104-7110-406b-a179-aa495452ae74'
(
    `date` Date,
    `counterid` UInt8,
    `timestamp` DateTime('UTC'),
    `userid` UInt64,
    `sessionid` UInt64,
    `path1` LowCardinality(String),
    `path2` LowCardinality(String),
    `utm_medium` LowCardinality(String),
    `utm_source` LowCardinality(String),
    `ref_domain` LowCardinality(String),
    `external_userid` UInt64,
    `ip_hash` UInt32,
    `useragentid` UInt64,
    `device_type` FixedString(1),
    `products` Nested(id UInt32, price UInt32, quantity UInt16, category UInt64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (counterid, userid, sessionid, timestamp)
SAMPLE BY userid
SETTINGS disk = disk(
    type=cache,
    max_size='2Gi',
    path='/tmp/custom_disk_cache/',
    disk=disk(
        type = s3_plain, readonly = true, 
        endpoint = 'https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/hits_retail2/', 
        no_sign_request = true
        )
);
```

### **Some queries**

1. Data discovery: check how some user session look like.

```sql
SELECT * FROM hits_retail WHERE sessionid = 933502217491 ORDER BY timestamp;
```

Here, you can see that the user came to the category page from Facebook, visited the product page, added the product to the basket, and processed the checkout.

2. Data discovery: counts.

```sql
select count(), uniq(userid), min(timestamp), max(timestamp), uniq(sessionid), uniq(ip_hash), uniq(useragentid), uniq(external_userid), uniqArray(products.id), uniqArray(products.category) from hits_retail\G

Row 1:

──────

count():                      68984862
uniq(userid):                 1947006
min(timestamp):               2016-10-22 00:00:00
max(timestamp):               2017-03-06 23:59:58
uniq(sessionid):              10423974
uniq(ip_hash):                1178207
uniq(useragentid):            421746
uniq(external_userid):        154900
uniqArray(products.id):       38677
uniqArray(products.category): 1722
```

3. Paths

```sql
SELECT path1, count(), uniq(path2),
    anyIf(path2, path2 != '') AS path2_sample,
    uniq(userid)
FROM hits_retail
GROUP BY path1
ORDER BY path1 ASC

┌─path1────┬──count()─┬─uniq(path2)─┬─path2_sample──┬─uniq(userid)─┐
│          │    67422 │           1 │               │        36307 │
│ account  │  4125219 │           7 │ promo         │       261694 │
│ basket   │  7131032 │           3 │ view          │       412862 │
│ category │ 28231276 │        1091 │ 423457        │      1518380 │
│ checkout │  1611928 │           5 │ shipping      │       197824 │
│ home     │  2314882 │           1 │               │       456575 │
│ other    │   248645 │          14 │ return_policy │       111499 │
│ product  │ 23958926 │       22228 │ 3480476772    │      1397218 │
│ purchase │   257703 │           1 │               │       164570 │
│ search   │  1037829 │       88418 │ BE168         │       180941 │
└──────────┴──────────┴─────────────┴───────────────┴──────────────┘
```

4. Referrer domains by popularity. As you can see, some domains and strings are human-readable, while others are obfuscated.

```sql
SELECT ref_domain, count()
FROM hits_retail
GROUP BY ref_domain
ORDER BY count() DESC
LIMIT 30

┌─ref_domain──────────────────────────────┬──count()─┐
│ www.altinity.test                       │ 53319058 │
│                                         │  5300385 │
│ m.facebook.com                          │  3069835 │
│ www.google.com                          │  2751079 │
│ 35.B6E.AD                               │  1573088 │
│ 35.C1B.AD                               │   528226 │
│ www.facebook.com                        │   527622 │
│ com.google.android.gm                   │   374867 │
│ googleads.g.doubleclick.net             │   148473 │
│ 886.9E0.AD                              │   120500 │
│ com.google.android.googlequicksearchbox │   117161 │
│ l.facebook.com                          │   106137 │
│ 886.30C.AD                              │    99842 │
│ F5.50CD.32                              │    85797 │
│ 35.21.AD                                │    67121 │
│ 35.6985.AD                              │    64696 │
│ 1E.F435.6D                              │    50850 │
│ tpc.googlesyndication.com               │    45828 │
│ www.googleadservices.com                │    42951 │
│ rdi.eu.criteo.com                       │    35973 │
│ ads.eu.criteo.com                       │    34289 │
│ instagram.com                           │    32156 │
│ lm.facebook.com                         │    30974 │
│ www.bing.com                            │    29757 │
│ 35.ED4.AD                               │    26801 │
│ youtube.com                             │    23427 │
│ mail.google.com                         │    22440 │
│ 35.CBE2.AD                              │    22193 │
│ AF3.CBCF.6B4                            │    18125 │
│ 019.55.AD                               │    16190 │
└─────────────────────────────────────────┴──────────┘
```

5. Device types

```sql
SELECT device_type, count()
FROM hits_retail
GROUP BY device_type

┌─device_type─┬──count()─┐
│             │     5944 │
│ c           │ 27613759 │
│ m           │ 38029082 │
│ t           │  3336077 │
└─────────────┴──────────┘
```

6. Product categories: highest and lowest price, number of products, number of sales

```sql
SELECT products.category,
  min(products.price) as lowest_price_in_category,
  max(products.price) as highest_price_in_category,
  uniq(products.id) as number_of_product_in_category,
  uniqIf( tuple(sessionid,timestamp), path1 = 'purchase' ) uniq_sales
FROM hits_retail
ARRAY JOIN products
GROUP BY products.category
ORDER BY uniq_sales DESC
LIMIT 20

┌─products.category─┬─min(products.price)─┬─max(products.price)─┬─count()─┐
│            294259 │                1313 │                1313 │   44969 │
│            632970 │                1313 │                1313 │   36685 │
│            790441 │                1696 │               11413 │   35790 │
│            276166 │                1313 │                1313 │   19547 │
│            414620 │                1313 │                1313 │   17693 │
│            277808 │                1313 │                1313 │   16661 │
│            176742 │                1313 │                1313 │   16436 │
│             62492 │                1666 │                4847 │   16317 │
│              7679 │                1313 │                1313 │   15744 │
│            879711 │                1313 │                1313 │   13053 │
│            823994 │                2221 │               10403 │   12565 │
│            674233 │                1313 │                1313 │    9905 │
│            494602 │                1813 │                3735 │    9716 │
│            516899 │                1313 │                1313 │    8624 │
│             59883 │                1918 │                5858 │    8547 │
│            418310 │                1868 │                5858 │    8448 │
│           1025019 │                1313 │                1313 │    8296 │
│           1021110 │                1313 │                1313 │    8199 │
│            777204 │                2120 │                5858 │    7841 │
│            356746 │                1544 │                4342 │    7362 │
└───────────────────┴─────────────────────┴─────────────────────┴─────────┘
```

7. Number of purchases per weekday (1=Monday, 7=Saturday)

```sql
SELECT toDayOfWeek(date) AS week_day, count()
FROM hits_retail
WHERE path1 = 'purchase'
GROUP BY week_day
ORDER BY week_day ASC

┌─week_day─┬─count()─┐
│        1 │   33103 │
│        2 │   34914 │
│        3 │   37785 │
│        4 │   42057 │
│        5 │   37292 │
│        6 │   30334 │
│        7 │   42218 │
└──────────┴─────────┘
```

8. More queries examples are here:
https://gist.github.com/filimonov/f56ead8ba91d525b99654770fa56f57c

## **Summary**

To illustrate different usage scenarios, different datasets are needed, and it's simply not possible to have one 'silver bullet' dataset that will show the whole range of ClickHouse possibilities.

Of course, as with any generated data, it's imperfect, but we've tried to make it good enough to illustrate some important ClickHouse use cases.

That dataset will be used in a few upcoming articles, showing some advanced possibilities of ClickHouse SQL extensions.