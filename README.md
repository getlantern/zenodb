# zenodb

zenodb is a Go-based embeddable time series database optimized for performing
aggregated analytical SQL queries on dimensional data.  It was developed to
replace [influxdb](https://github.com/influxdata/influxdb/) as a repository for
client and server metrics at [Lantern](https://www.getlantern.org).

## Current Features

 * No limits on the number of dimensions
 * SQL-based query language including GROUP BY and HAVING support
 * Auto-correlation
 * Reasonably efficient storage model
 * Seems pretty fast
 * Some unit tests

## Still Missing

 * Write Ahead Log
 * More unit tests

## Quick Start

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
    SELECT
      success_count,
      error_count
    FROM inbound
    GROUP BY *, period(5m)
```

Open three terminals

Terminal 1

```bash
# Start the database
zeno-quickstart > zeno
DEBUG zenodb: schema.go:76 Creating table 'combined' as
SELECT
  success_count,
  error_count
FROM inbound GROUP BY *, period(5m)
DEBUG zenodb: table.go:101 MinFlushLatency disabled
DEBUG zenodb: table.go:105 MaxFlushLatency disabled
DEBUG zenodb: schema.go:81 Created table combined
DEBUG zenodb: zenodb.go:40 Dir: zenodb    SchemaFile: schema.yaml
Opened database at zenodb
Listening for gRPC connections at 127.0.0.1:17712
Listening for HTTP connections at 127.0.0.1:17713
```

Terminal 2

```bash
# Submit some data via the REST API. Omit the ts parameter to use current time.
> curl -i -H "Content-Type: application/json" -X POST -d '{"dims": {"server": "56.234.163.23", "path": "/index.html"}, "vals": {"error_count": 1, "success_count": 3}}' http://localhost:17713/insert/inbound
HTTP/1.1 201 Created
Date: Sun, 07 Aug 2016 12:47:21 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

Terminal 3

```bash
# Query the data
> zeno-cli
Will save history to /Users/ox.to.a.cart/Library/Application Support/zeno-cli/history
zeno-cli > SELECT _points, error_count, success_count, error_count / (error_count+success_count) AS error_rate FROM combined;
-------------------------------------------------
# As Of:      2016-08-07 06:45:00 -0500 CDT
# Until:      2016-08-07 07:45:00 -0500 CDT
# Resolution: 5m0s
# Group By:   path server

# Query Runtime:  161.367µs

# Key Statistics
#   Scanned:       1
#   Filter Pass:   0
#   Read Value:    3
#   Valid:         3
#   In Time Range: 3
-------------------------------------------------

# time                             path           server               _points    error_count    success_count    error_rate
Sun, 07 Aug 2016 07:45:00 CDT      /index.html    56.234.163.23         1.0000         1.0000           3.0000        0.2500
```

Now run the same insert again and query again. Pro tip - zeno-cli has a history,
so try the up-arrow or `Ctrl+R`.

Notice that the count of points and total counts have gone up, but the rate
remains the same.

```bash
# time                             path           server               _points    error_count    success_count    error_rate
Sun, 07 Aug 2016 07:45:00 CDT      /index.html    56.234.163.23         2.0000         2.0000           6.0000        0.2500
```

Try inserting again with a higher error count:

```bash
> curl -i -H "Content-Type: application/json" -X POST -d '{"dims": {"server": "56.234.163.23", "path": "/index.html"}, "vals": {"error_count": 3, "success_count": 3}}' http://localhost:17713/insert/inbound
```

Now the rate has changed:

```
# time                             path           server               _points    error_count    success_count             error_rate
Sun, 07 Aug 2016 07:50:00 CDT      /index.html    56.234.163.23         3.0000         5.0000           9.0000                 0.3571
```

## Acknowledgements

 * [RocksDB](http://rocksdb.org/) - The storage engine
 * [gorocksdb](https://github.com/tecbot/gorocksdb) - Go language bindings for RocksDB
 * [sqlparser](https://github.com/xwb1989/sqlparser) - Go SQL parser
 * [govaluate](https://github.com/Knetic/govaluate) - Go expression evaluation
