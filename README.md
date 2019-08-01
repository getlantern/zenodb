ZenoDB [![Travis CI Status](https://travis-ci.org/getlantern/zenodb.svg?branch=master)](https://travis-ci.org/getlantern/zenodb)&nbsp;[![Coverage Status](https://coveralls.io/repos/getlantern/zenodb/badge.png)](https://coveralls.io/r/getlantern/zenodb)&nbsp;[![GoDoc](https://godoc.org/github.com/getlantern/zenodb?status.png)](http://godoc.org/github.com/getlantern/zenodb)&nbsp;[![Sourcegraph](https://sourcegraph.com/github.com/getlantern/zenodb/-/badge.svg)](https://sourcegraph.com/github.com/getlantern/zenodb?badge)
==========

ZenoDB is a Go-based embeddable time series database optimized for performing
aggregated analytical SQL queries on dimensional data.  It was developed to
replace [influxdb](https://github.com/influxdata/influxdb/) as a repository for
client and server metrics at [Lantern](https://www.getlantern.org).

## Dependencies
This project uses Go modules to manage dependencies. If running a Go version
prior to 1.13, you can enable Go modules using the environment variable
`GO111MODULE=on`, like:

```
GO111MODULE=on go install github.com/getlantern/zenodb/cmd/zeno
```

## Current Features

 * No limits on the number of dimensions
 * SQL-based query language including GROUP BY and HAVING support
 * Auto-correlation
 * Reasonably efficient storage model
 * (Mostly) parallel query processing
 * Crosstab queries
 * FROM subqueries
 * Write-ahead Log
 * Seems pretty fast
 * Materialized views (with historical data from write-ahead log)
 * Some unit tests
 * Limit query memory consumption to avoid OOM killer
 * Multi-leader, multi-follower architecture
 
## Future Stuff

 * Auto cleanup of Write-ahead Log
 * Harmonize field vs column language
 * More unit tests and general code cleanup
 * Byte array buffers to avoid allocations for sequences and ByteMaps
 * Smart sorting - e.g. only sort data files if a substantial number of new keys have been added
 * More validations/error checking
 * TLS in HTTP
 * Stored statistics (database-level, table-level, size, throughput, dimensions, etc.)
 * Optimized queries using expression references (avoid recomputing same expression when referenced multiple times in same row)
 * Completely parallel query processing
 * Interruptible queries using Context
 * User-level authentication/authorization
 * Multi-dimensional crosstab queries
 * Read-only query server replication using rsync?

## Standalone Quick Start

In this tutorial, you will:

* Run Zeno as a standalone database
* Insert data into zeno using the RESTful HTTP API
* Query zeno using the zeno-cli

You will learn how to use zeno to:

* Aggregate multi-dimensional data
* Correlate different kinds of data by inserting into a single table
* Correlate data using the `IF` function
* Derive data by performing calculations on existing data
* Sort data
* Filter data based on the results of your aggregations

Install [Go](https://golang.org/doc/install) if you haven't already.

```bash
GO111MODULE=on go install github.com/getlantern/zenodb/cmd/zeno
GO111MODULE=on go install github.com/getlantern/zenodb/cmd/zeno-cli
```

Make a working directory (e.g. '~/zeno-quickstart').  In here, create a
`schema.yaml` like the below to configure your database:

```sql
combined:
  retentionperiod: 1h
  sql: >
    SELECT
      requests,
      AVG(load_avg) AS load_avg
    FROM inbound
    GROUP BY *, period(5m)
```

This schema creates a *table* called `combined` which is filled by selecting
data from the *stream* `inbound`. `combined` keeps track of the `requests` and
`load_avg` values and includes all dimensions from the `inbound` stream.  It
groups data into 5 minute buckets. The `requests` column is grouped using the
`SUM` aggregation operator, which is the default if no operator is specified.
`load_avg` on the other hand is aggregated as an average.

**Core Concept** - Zeno does not store individual points, everything is stored
in aggregated form.

Open three terminals

Terminal 1

```bash
> # Start the database
> cd ~/zeno-quickstart
> zeno
DEBUG zenodb: schema.go:77 Creating table 'combined' as
SELECT
  requests,
  AVG(load_avg) AS load_avg
FROM inbound GROUP BY *, period(5m)
DEBUG zenodb: schema.go:78 MaxMemStoreBytes: 1 B    MaxFlushLatency: 0s    MinFlushLatency: 0s
DEBUG zenodb: table.go:118 MinFlushLatency disabled
DEBUG zenodb: table.go:122 MaxFlushLatency disabled
DEBUG zenodb: schema.go:83 Created table combined
DEBUG zenodb: zenodb.go:63 Enabling geolocation functions
DEBUG zenodb.combined: row_store.go:111 Will flush after 2562047h47m16.854775807s
DEBUG zenodb: zenodb.go:75 Dir: /Users/ox.to.a.cart//zeno-quickstart    SchemaFile: /Users/ox.to.a.cart//zeno-quickstart/schema.yaml
Opened database at /Users/ox.to.a.cart//zeno-quickstart
Listening for gRPC connections at 127.0.0.1:17712
Listening for HTTP connections at 127.0.0.1:17713
```

Terminal 2

```bash
> # Submit some data via the REST API. Omit the ts parameter to use current time.
> curl -i -H "Content-Type: application/json" -X POST -d '{"dims": {"server": "56.234.163.23", "path": "/index.html", "status": 200}, "vals": {"requests": 56}}
{"dims": {"server": "56.234.163.23", "path": "/login", "status": 200}, "vals": {"requests": 34}}
{"dims": {"server": "56.234.163.23", "path": "/login", "status": 500}, "vals": {"requests": 12}}
{"dims": {"server": "56.234.163.23"}, "vals": {"load_avg": 1.7}}
{"dims": {"server": "56.234.163.24", "path": "/index.html", "status": 200}, "vals": {"requests": 523}}
{"dims": {"server": "56.234.163.24", "path": "/login", "status": 200}, "vals": {"requests": 411}}
{"dims": {"server": "56.234.163.24", "path": "/login", "status": 500}, "vals": {"requests": 28}}
{"dims": {"server": "56.234.163.24"}, "vals": {"load_avg": 0.3}}' -k https://localhost:17713/insert/inbound
HTTP/1.1 201 Created
Date: Mon, 29 Aug 2016 03:00:38 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

Notice that:

* You're inserting into a the stream `inbound` not the table `combined`
* You can batch insert multiple points in a single HTTP request
* You can insert heterogenous data like HTTP response statuses and load averages
  into a single stream, thereby automatically correlating the data on any shared
  dimensions (bye bye JOINs!).

Terminal 3

*SQL*

```sql
SELECT
  _points,
  requests,
  load_avg
FROM combined
GROUP BY *
ORDER BY requests DESC
```

*zeno-cli*

```sql
> # Query the data
> zeno-cli -insecure -fresh
Will save history to /Users/ox.to.a.cart/Library/Application Support/zeno-cli/history
zeno-cli > SELECT _points, requests, load_avg FROM combined GROUP BY * ORDER BY requests DESC;
# time                             path           server           status        _points    requests    load_avg
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.24    200            1.0000    523.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    200            1.0000    411.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.23    200            1.0000     56.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    200            1.0000     34.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    500            1.0000     28.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    500            1.0000     12.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.23    <nil>          1.0000      0.0000      1.7000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.24    <nil>          1.0000      0.0000      0.3000
```

Notice that:

* Dimensions are included in the result based on the GROUP BY, you don't include
  them in the SELECT expression.
* There's a built-in field `_points` that gives a count of the number of points
  that were inserted.

Now run the same insert again.

Then run the same query again.

**Pro tip** - zeno-cli has a history, so try the up-arrow or `Ctrl+R`.

*zeno-cli*

```sql
zeno-cli > SELECT _points, requests, load_avg FROM combined GROUP BY * ORDER BY requests DESC;
# time                             path           server           status        _points     requests    load_avg
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.24    200            2.0000    1046.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    200            2.0000     822.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.23    200            2.0000     112.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    200            2.0000      68.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    500            2.0000      56.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    500            2.0000      24.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.23    <nil>          2.0000       0.0000      1.7000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.24    <nil>          2.0000       0.0000      0.3000
```

As long as you're submitted the 2nd batch of data soon after the first, you
should see that the number of rows hasn't changed, Zeno just aggregated the data
on the existing timestamps.  The requests figures all doubled, since these are
aggregated as a `SUM`. The load_avg figures remained unchanged since they're
being aggregated as an `AVG`.

Note - if enough time has elapsed that we have a new timestamp, you will see
additional rows like this:

```
# time                             path           server           status        _points    requests    load_avg
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.24    200            1.0000    523.0000      0.0000
Mon, 29 Aug 2016 03:05:00 UTC      /index.html    56.234.163.24    200            1.0000    523.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    200            1.0000    411.0000      0.0000
Mon, 29 Aug 2016 03:05:00 UTC      /login         56.234.163.24    200            1.0000    411.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.23    200            1.0000     56.0000      0.0000
Mon, 29 Aug 2016 03:05:00 UTC      /index.html    56.234.163.23    200            1.0000     56.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    200            1.0000     34.0000      0.0000
Mon, 29 Aug 2016 03:05:00 UTC      /login         56.234.163.23    200            1.0000     34.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    500            1.0000     28.0000      0.0000
Mon, 29 Aug 2016 03:05:00 UTC      /login         56.234.163.24    500            1.0000     28.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    500            1.0000     12.0000      0.0000
Mon, 29 Aug 2016 03:05:00 UTC      /login         56.234.163.23    500            1.0000     12.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.23    <nil>          1.0000      0.0000      1.7000
Mon, 29 Aug 2016 03:05:00 UTC      <nil>          56.234.163.23    <nil>          1.0000      0.0000      1.7000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.24    <nil>          1.0000      0.0000      0.3000
Mon, 29 Aug 2016 03:05:00 UTC      <nil>          56.234.163.24    <nil>          1.0000      0.0000      0.3000
```

**Core Concept** - Zeno knows how to aggregate fields based on the schema, so
you don't need to include aggregation operators in your query.  What happens if
we try to query for `SUM(load_avg)`?

*zeno-cli*

```sql
zeno-cli > SELECT _points, requests, SUM(load_avg) AS load_avg FROM combined GROUP BY * ORDER BY requests DESC;
rpc error: code = 2 desc = No column found for load_avg (SUM(load_avg))
```

The underlying column is an `AVG(load_avg)`, so taking a SUM is not possible!

Sometimes, it's useful to show a dimension in columns rather than rows. You can
do this using the `CROSSTAB` function.

```sql
SELECT
  requests,
  load_avg
FROM combined
GROUP BY server, CROSSTAB(path)
ORDER BY requests;
```

```sql
zeno-cli > SELECT requests, load_avg FROM combined GROUP BY server, CROSSTAB(path) ORDER BY requests;
# time                             server                  requests    requests     requests    load_avg    load_avg
#                                                       /index.html      /login      *total*       <nil>     *total*
Mon, 29 Aug 2016 03:05:00 UTC      56.234.163.23           112.0000     92.0000     204.0000      1.7000      1.7000
Mon, 29 Aug 2016 03:05:00 UTC      56.234.163.24          1046.0000    878.0000    1924.0000      0.3000      0.3000
```

Notice how there's a second header row now that shows the different values of
path. Notice also how paths that don't have any data are not shown, and notice
that a *total* column is automatically included for each field.

Now let's do some correlation using the `IF` function.  `IF` takes two
parameters, a conditional expression that determines whether or not to include a
value based on its associated dimensions, and the value expression that selects
which column to include.

Let's say that we want to get the error rate, defined as the number of non-200
statuses versus total requests:

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_rate,
  load_avg
FROM combined
GROUP BY *
ORDER BY error_rate DESC
```

*zeno-cli*

```sql
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_rate, load_avg FROM combined GROUP BY * ORDER BY error_rate DESC;
# time                             path           server           status         errors     requests    error_rate    load_avg
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    500           56.0000      56.0000        1.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    500           24.0000      24.0000        1.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.23    200            0.0000      68.0000        0.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.23    <nil>          0.0000       0.0000        0.0000      1.7000
Mon, 29 Aug 2016 03:00:00 UTC      <nil>          56.234.163.24    <nil>          0.0000       0.0000        0.0000      0.3000
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.23    200            0.0000     112.0000        0.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /login         56.234.163.24    200            0.0000     822.0000        0.0000      0.0000
Mon, 29 Aug 2016 03:00:00 UTC      /index.html    56.234.163.24    200            0.0000    1046.0000        0.0000      0.0000
```

Okay, this distinguishes between errors and other requests, but errors and other
requests aren't being correlated yet so the error_rate isn't useful. Notice also
that load_avg is separate from the requests measurements.  That's because we're
still implicitly grouping on status and path, so error rows are separate from
success rows and load_avg rows (which are only associated with servers, not
paths) are separate from everything else.

So instead, let's group only by server:

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_rate,
  load_avg
FROM combined
GROUP BY server
ORDER BY error_rate DESC
```

*zeno-cli*

```sql
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_rate, load_avg FROM combined GROUP BY server ORDER BY error_rate DESC;
# time                             server                errors     requests    error_rate    load_avg
Mon, 29 Aug 2016 03:00:00 UTC      56.234.163.23        24.0000     204.0000        0.1176      1.7000
Mon, 29 Aug 2016 03:00:00 UTC      56.234.163.24        56.0000    1924.0000        0.0291      0.3000
```

That looks better!  We're getting a meaningful error rate, and we can even see
that there's a correlation between the error_rate and the load_avg.

**Challenge** - What calculation would yield a meaningful understanding of the
relationship between error_rate and load_avg?

Now, if we had a ton of servers, we would really only be interested in the ones
with the top error rates.  We could handle that either with a `LIMIT` clause:

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_rate,
  load_avg
FROM combined
GROUP BY server
ORDER BY error_rate DESC
LIMIT 1
```

*zeno-cli*

```sql
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_rate, load_avg FROM combined GROUP BY server ORDER BY error_rate DESC LIMIT 1;
# time                             server                errors    requests    error_rate    load_avg
Mon, 29 Aug 2016 03:00:00 UTC      56.234.163.23        24.0000    204.0000        0.1176      1.7000
```

Or you can we can use the `HAVING` clause to filter based on the actual data:

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_rate,
  load_avg
FROM combined
GROUP BY server
HAVING error_rate > 0.1
ORDER BY error_rate DESC
```

*zeno-cli*

```sql
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_rate, load_avg FROM combined GROUP BY server HAVING error_rate > 0.1 ORDER BY error_rate DESC;
# time                             server                errors    requests    error_rate    load_avg
Mon, 29 Aug 2016 03:05:00 UTC      56.234.163.23        24.0000    204.0000        0.1176      1.7000
```



There!  You've just aggregated, correlated and gained valuable insights into
your server infrastructure.  At [Lantern](https://www.getlantern.org) we do this
sort of stuff with data from thousands of servers and millions of clients!

## Schema

ZenoDB relies on a schema file (by default `schema.yaml`).

### Example: How to add a view

A view is really a language construct for creating a table whose properties are derived from an existing table.  The data stream between the view and the table it inherits from is the same, however, it's stored separately. Consequently, a view can have different (and finer) granularity than its parent table.

This is an example of a view defined in the YAML Schema:

```
emojis_fetched:
  view:             true
  retentionperiod:  168h
  backfill:         6h
  minflushlatency:  1m
  maxflushlatency:  1h
  partitionby:      [client_ip]
  sql: >
    SELECT success_count, error_count, error_rate, emojis_fetched
      FROM core
      WHERE client_ip LIKE ‘192.-%’
      GROUP BY client_ip, period(1h)
```

We start with the name of the view:

`emojis_fetched`

This means that the data for this table comes from another table or view, rather than an input stream:

`view: true`

This means that we keep 1 week worth of history

`retentionperiod: 168h`

This means that we won’t flush the memstore more frequently than every 1 minute:

`minflushlatency: 1m`

And flush at least every hour:

`maxflushlatency:  1h`

Flusing means storing the data held in memory until now, and saving it to the permanent on-disk storage for later retrieval.

The physical storage happens on the follower nodes, so Zenodb needs to know how to distribute that data across nodes:

`partitionby: [client_ip]`

The next part is the definition of the contents of the table/view:

```
  sql: >
    SELECT success_count, error_count, error_rate, emojis_fetched
      FROM core
      WHERE client_ip LIKE ‘192.-%’
      GROUP BY client_ip, period(1h)
```

The `SELECT` row are the fields. In this case, success_count, error_count, error_rate and emojis_fetched. The rest of the statement are the grouping and filter operations, which are optional and can be adapted to the needs. This selects only measurements related to a group of IPs.

`WHERE client_ip LIKE ‘192.-%’`

For instance, if we wanted to change it to emojis:

`WHERE emojis_fetched LIKE ‘smile-%’`

This selects which dimensions to keep, and what resolution to use. This is usually the hardest part of creating a view, because you need to anticipate what questions will be asked:

`GROUP BY client_ip, period(1h)`

## Functions

TODO - fill out function reference

## Subqueries

TODO - explain how subqueries work

## Embedding

Check out the [zenodbdemo](zenodbdemo/zenodbdemo.go) for an example of how to
embed zenodb.

## Implementation Notes

### Sequences

These are the central units of data storage in Zenodb: https://github.com/getlantern/zenodb/blob/master/encoding/seq.go

In short, we group by dimension, and then we summarize/roll-up fields. The summarization is done using an aggregation function like SUM, AVG, MIN, MAX, etc. That is key aspect to how Zenodb manages to discard data and achieve compression.
For example, for a new view, let’s say we’re not grouping by anything (that would be `GROUP BY _, PERIOD(1h)`), and let’s look just at the success_count field, in storage, we would have a single row with no dimensions and an array of aggregated success_counts by time period:

`success_count: [4000, 5000]`

That would be the result of a series of points that add up to 4000 in the first period, and to 5000 in the second period (of 1h)

Now let’s say that we did `GROUP BY geo_country, PERIOD(1h)`

And let’s say that success_counts are evenly split between US and AU, then we would have two rows:

```
geo_country: US   success_count: [2000, 2500]
geo_country: AU   success_count: [2000, 2500]
```

One important aspect is that each field is actually not just an array, the array is actually preceded by the timestamp high water mark, so it would actually be something like this:

`geo_country: US   success_count: 20180118T05:00Z[1000, 1500]`

### Views

Since everything in zenodb comes in through input streams, views cannot actually be constructed from the underlying tables, so they need to be stored independently. This allows for views to have different granularities that the tables/views they are referring to. In other words, at runtime, views are actually just like tables that pull from that same input stream, the only difference is that when you define a view, it can take into account knowledge from the definition of the underlying table.

## Clustering

### Performance timestamps

* Partition on high cardinality fields/combinations that you frequently query
* Don't partition on low-cardinality fields as these will tend to hotspot one
  one or another partition and slow down synchronization from the leader.
* Don't partition on too many different fields/combinations is this will
  increase amount of data that each follower has to synchronize.

## Acknowledgements

 * [sqlparser](https://github.com/xwb1989/sqlparser) - Go SQL parser
