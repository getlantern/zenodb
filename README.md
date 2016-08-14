# ZenoDB

ZenoDB is a Go-based embeddable time series database optimized for performing
aggregated analytical SQL queries on dimensional data.  It was developed to
replace [influxdb](https://github.com/influxdata/influxdb/) as a repository for
client and server metrics at [Lantern](https://www.getlantern.org).

## Current Features

 * No limits on the number of dimensions
 * SQL-based query language including GROUP BY and HAVING support
 * Auto-correlation
 * Reasonably efficient storage model
 * (Mostly) parallel query processing
 * Seems pretty fast
 * Some unit tests

## Future Stuff

 * Write Ahead Log
 * More unit tests and general code cleanup
 * Populate views with historical data upon creation
 * Limit query memory consumption to avoid OOM killer
 * More validations/error checking
 * TLS in RPC
 * TLS in HTTP
 * Stored statistics (database-level, table-level, size, throughput, dimensions, etc.)
 * Optimized queries using expression references (avoid recomputing same expression when referenced multiple times in same row)
 * Completely parallel query processing
 * Interuptable queries using Context
 * User-level authentication/authorization
 * Crosstab queries
 * Read-only query server replication using rsync?

## Standalone Quick Start

Install [Go](https://golang.org/doc/install) if you haven't already.

```bash
go install github.com/getlantern/zenodb/zeno
go install github.com/getlantern/zenodb/zeno-cli
```

Make a working directory (e.g. '~/zeno-quickstart').  In here, create a
`schema.yaml` like the below to configure your database:

```
combined:
  maxmemstorebytes: 1
  retentionperiod: 1h
  sql: >
    SELECT requests
    FROM inbound
    GROUP BY *, period(5m)
```

Open three terminals

Terminal 1

```bash
> # Start the database
> cd ~/zeno-quickstart
> zeno
DEBUG zenodb: schema.go:77 Creating table 'combined' as
SELECT
  requests
FROM inbound GROUP BY *, period(5m)

DEBUG zenodb: schema.go:78 MaxMemStoreBytes: 1 B    MaxFlushLatency: 0    MinFlushLatency: 0
DEBUG zenodb: table.go:101 MinFlushLatency disabled
DEBUG zenodb: table.go:105 MaxFlushLatency disabled
DEBUG zenodb: schema.go:83 Created table combined
DEBUG zenodb: zenodb.go:43 Dir: zenodb    SchemaFile: schema.yaml
Opened database at zenodb
Listening for gRPC connections at 127.0.0.1:17712
DEBUG zenodb.combined: row_store.go:109 Will flush after 2562047h47m16.854775807s
Listening for HTTP connections at 127.0.0.1:17713
```

Terminal 2

```bash
> # Submit some data via the REST API. Omit the ts parameter to use current time.
> curl -i -H "Content-Type: application/json" -X POST -d '{"dims": {"server": "56.234.163.23", "path": "/index.html", "status": 200}, "vals": {"requests": 56}}
{"dims": {"server": "56.234.163.23", "path": "/login", "status": 200}, "vals": {"requests": 34}}
{"dims": {"server": "56.234.163.23", "path": "/login", "status": 500}, "vals": {"requests": 12}}
{"dims": {"server": "56.234.163.24", "path": "/index.html", "status": 200}, "vals": {"requests": 523}}
{"dims": {"server": "56.234.163.24", "path": "/login", "status": 200}, "vals": {"requests": 411}}
{"dims": {"server": "56.234.163.24", "path": "/login", "status": 500}, "vals": {"requests": 28}}' http://localhost:17713/insert/inbound
HTTP/1.1 201 Created
Date: Sun, 14 Aug 2016 01:47:48 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

Terminal 3

*SQL*

```sql
SELECT
  _points,
  requests
FROM combined
GROUP BY *
ORDER BY requests DESC
```

*zeno-cli*

```bash
> # Query the data
> zeno-cli
Will save history to /Users/ox.to.a.cart/Library/Application Support/zeno-cli/history
zeno-cli > SELECT _points, requests FROM combined GROUP BY * ORDER BY requests DESC;
-------------------------------------------------
# As Of:      Sat, 13 Aug 2016 01:45:00 UTC
# Until:      Sun, 14 Aug 2016 01:45:00 UTC
# Resolution: 5m0s
# Group By:   path server status

# Query Runtime:  194.789µs

# Key Statistics
#   Scanned:       6
#   Filter Pass:   0
#   Read Value:    12
#   Valid:         12
#   In Time Range: 12
-------------------------------------------------

# time                             path           server           status        _points    requests
Sun, 14 Aug 2016 01:45:00 UTC      /index.html    56.234.163.24    200            1.0000    523.0000
Sun, 14 Aug 2016 01:45:00 UTC      /login         56.234.163.24    200            1.0000    411.0000
Sun, 14 Aug 2016 01:45:00 UTC      /index.html    56.234.163.23    200            1.0000     56.0000
Sun, 14 Aug 2016 01:45:00 UTC      /login         56.234.163.23    200            1.0000     34.0000
Sun, 14 Aug 2016 01:45:00 UTC      /login         56.234.163.24    500            1.0000     28.0000
Sun, 14 Aug 2016 01:45:00 UTC      /login         56.234.163.23    500            1.0000     12.0000
```

Notice that we can query the built-in field `_points` that gives a count of the
number of points that were inserted.

Now run the same insert again.  Then run the same query again.
**Pro tip** zeno-cli has a history, so try the up-arrow or `Ctrl+R`.

*zeno-cli*

```bash
# time                             path           server           status        _points     requests
Sun, 14 Aug 2016 01:50:00 UTC      /index.html    56.234.163.24    200            2.0000    1046.0000
Sun, 14 Aug 2016 01:50:00 UTC      /login         56.234.163.24    200            2.0000     822.0000
Sun, 14 Aug 2016 01:50:00 UTC      /index.html    56.234.163.23    200            2.0000     112.0000
Sun, 14 Aug 2016 01:50:00 UTC      /login         56.234.163.23    200            2.0000      68.0000
Sun, 14 Aug 2016 01:50:00 UTC      /login         56.234.163.24    500            2.0000      56.0000
Sun, 14 Aug 2016 01:50:00 UTC      /login         56.234.163.23    500            2.0000      24.0000
```

Notice that the number of rows hasn't changed, Zeno just aggregated the data
on the existing timestamps.

Now let's do some correlation.  Let's say that we want to get the error rate,
defined as the number of non-200 statuses versus total requests.

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_Rate
FROM combined
GROUP BY *
ORDER BY error_rate DESC
```

*zeno-cli*

```bash
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_Rate FROM combined GROUP BY * ORDER BY error_rate DESC;
# time                             path           server           status         errors     requests    error_rate
Sun, 14 Aug 2016 01:55:00 UTC      /login         56.234.163.23    500           24.0000      24.0000        1.0000
Sun, 14 Aug 2016 01:55:00 UTC      /login         56.234.163.24    500           56.0000      56.0000        1.0000
Sun, 14 Aug 2016 01:55:00 UTC      /login         56.234.163.23    200            0.0000      68.0000        0.0000
Sun, 14 Aug 2016 01:55:00 UTC      /index.html    56.234.163.23    200            0.0000     112.0000        0.0000
Sun, 14 Aug 2016 01:55:00 UTC      /login         56.234.163.24    200            0.0000     822.0000        0.0000
Sun, 14 Aug 2016 01:55:00 UTC      /index.html    56.234.163.24    200            0.0000    1046.0000        0.0000
```

Okay, this distinguishes between errors and overall requests, but errors and
other requests aren't being correlated yet. That's because we're still
implicitly grouping on status, so error rows are separate from success rows.
Instead, let's group only by server and path.

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_Rate
FROM combined
GROUP BY server, path
ORDER BY error_rate DESC
```

*zeno-cli*

```bash
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_Rate FROM combined GROUP BY server, path ORDER BY error_rate DESC;
# time                             server           path                errors     requests    error_rate
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.23    /login             24.0000      92.0000        0.2609
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.24    /login             56.0000     878.0000        0.0638
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.24    /index.html         0.0000    1046.0000        0.0000
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.23    /index.html         0.0000     112.0000        0.0000
```

That looks better!  We could also look at rates just by server:

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_Rate
FROM combined
GROUP BY server
ORDER BY error_rate DESC
```

*zeno-cli*

```bash
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_Rate FROM combined GROUP BY server ORDER BY error_rate DESC;
# time                             server                errors     requests    error_rate
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.23        24.0000     204.0000        0.1176
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.24        56.0000    1924.0000        0.0291
```

Now, if we had a ton of servers, we would really only be interested in the ones
with the top error rates.  We could handle that either with a limit clause:

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_Rate
FROM combined
GROUP BY server
ORDER BY error_rate DESC
LIMIT 1
```

*zeno-cli*

```bash
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_Rate FROM combined GROUP BY server ORDER BY error_rate DESC LIMIT 1;
# time                             server                errors    requests    error_rate
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.23        24.0000    204.0000        0.1176
```

Or you can also use the HAVING clause to place limits based on the actual data:

*sql*

```sql
SELECT
  IF(status <> 200, requests) AS errors,
  requests,
  errors / requests AS error_Rate
FROM combined
GROUP BY server
HAVING error_rate > 0.1
ORDER BY error_rate DESC
```

*zeno-cli*

```bash
zeno-cli > SELECT IF(status <> 200, requests) AS errors, requests, errors / requests AS error_Rate FROM combined GROUP BY server HAVING error_rate > 0.1 ORDER BY error_rate DESC;
# time                             server                errors    requests    error_rate
Sun, 14 Aug 2016 01:55:00 UTC      56.234.163.23        24.0000    204.0000        0.1176
```

## Acknowledgements

 * [sqlparser](https://github.com/xwb1989/sqlparser) - Go SQL parser
