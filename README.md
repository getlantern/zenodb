# tibsdb

tibsdb is an embeddable time series database optimized for performing aggregating
analytical queries on dimensional data.  It was developed to replace
[influxdb](https://github.com/influxdata/influxdb/) as a repository for error
log metrics at [Lantern](https://www.getlantern.org).

## Current Features

 * No limits on the number of dimensions
 * SQL-based query language including GROUP BY and HAVING support
 * Reasonably efficient storage model
 * Seems pretty fast

## Still Missing

 * Write Ahead Log
 
## Acknowledgements

 * [RocksDB](http://rocksdb.org/) - The storage engine
 * [gorocksdb](https://github.com/tecbot/gorocksdb) - Go language bindings for RocksDB
 * [sqlparser](https://github.com/xwb1989/sqlparser) - Go SQL parser
 * [govaluate](https://github.com/Knetic/govaluate) - Go expression evaluation
