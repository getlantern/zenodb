.. zenodb documentation master file, created by
   sphinx-quickstart on Sun Apr  2 22:30:54 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. highlight:: guess

.. role:: sql(code)
  :language: sql

.. role:: go(code)
  :language: go

ZenoDB User Guide
=================

What is ZenoDB
--------------
ZenoDB is a semi-dynamically typed, embeddable, clusterable OLAP time-series
database that supports correlation of heterogeneous data and which uses SQL as
its query language. It includes a command-line client as well as a built-in web
reporting UI.

The Lantern project uses ZenoDB for monitoring its server infrastructure as well
as the health of client applications.

Time-Series
^^^^^^^^^^^
ZenoDB stores time-series data, which means data that includes a timestamp.

.. TIP::
   The raw data in a time-series is called *points*.

OLAP
^^^^
ZenoDB is an `OLAP <https://en.wikipedia.org/wiki/Online_analytical_processing>`_
database optimized for aggregating, drilling down and slicing and dicing multi-
dimensional data, of which time happens to be one dimension. ZenoDB accomplishes
this by storing data in pre-aggregated form and performing further aggregation
at query time.

Just like a traditional OLAP database, ZenoDB stores measures categorized by
associated dimensions.

.. TIP::
   In ZenoDB, measures are called *values*. All values are 64-bit floating point
   numbers.

Semi-dynamically Typed
^^^^^^^^^^^^^^^^^^^^^^
Dimensions in ZenoDB are dynamically typed. Points submitted to ZenoDB can
include an arbitrary number of dimensions.

Dimensions can be any of the following types:
 * nil (a.k.a. null)
 * bool
 * string
 * byte
 * uint16
 * uint32
 * uint64
 * uint (platform-dependent precision)
 * int8
 * int16
 * int32
 * int64
 * int (platform-dependent precision)
 * float32
 * float64
 * timestamp

Example dimensions might be a server IP address, a web path, or an HTTP status
code.

Values are always of type float64. Example values might be a response time or a
server load average.

In order to reduce storage requirements, speed up query processing and simplify
query writing, values in tables are statically specified as *fields* that
describe how the values are to be aggregated and otherwise transformed from
the raw values received as part of a time-series.

The supported aggregation functions are:

 * SUM (default) - simply totals the given value across all points
 * AVG - the arithmatic mean of the value across all points.
 * MIN - the minimum of a value across all points.
 * MAX - the maximum of a value across all points.

.. NOTE::
   All aggregation functions ignore points that do not contain the specified
   value.

Other transformations include arithmetic, logical and comparison functions.
Example fields might be:

 * :sql:`AVG(response_time_millis / 1000) AS response_time_seconds`
 * :sql:`SUM(status_code >= 200 AND status_code <= 299) AS success_count`
 * :code:`import stuff as thing`

Here is a code sample:: sql

  SELECT * FROM thing WHERE dude = 5

Correlation of Heterogenous Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Since dimensions in ZenoDB are dynamically typed, it is possible and easy to
include different kinds of data in the same time series and allow that data to
be correlated, for example

Embeddable
^^^^^^^^^^
ZenoDB can run standalone or be imbedded in another application using its Go
language API.

Clusterable
^^^^^^^^^^^
ZenoDB processing involves three main activities:

* Ingestion - consume streams of time-series data
* Storage - persist the pre-aggregated data to disk and load said data at query
            time.
* Querying - slice and dice the stored data.

Storage can be distributed to multiple nodes (followers).

Multiple nodes can be set up for query processing.

Currently, all ingestion happens on a single node.

Example
-------
Here are some specific points of a time-series. In a real-world scenario, this
wouldn't be enough data to draw any statistically significant conclusions, but
it's a useful example for understanding how ZenoDB works.

.. TIP::
   In the ZenoDB documentation, dimensions are flagged with a **(d)** to
   distinguish them from values.

====================  =============  ========   ===============  =============  ========
time (d)              server (d)     path (d)   status_code (d)  response_time  load_avg
====================  =============  ========   ===============  =============  ========
2017-04-01T16:00:00Z  56.234.163.23  /login     200                         10
2017-04-01T16:00:01Z  56.234.163.23  /login     200                         30
2017-04-01T16:00:00Z  56.234.163.23  /login     403                        400
2017-04-01T16:00:00Z  56.234.163.24  /login     200                       1000
2017-04-01T16:00:01Z  56.234.163.24  /login     200                       2000
2017-04-01T16:00:00Z  56.234.163.24  /login     403                        450
2017-04-01T16:00:00Z  56.234.163.23  /welcome   200                         50
2017-04-01T16:00:01Z  56.234.163.23  /welcome   200                        100
2017-04-01T16:00:00Z  56.234.163.23  /welcome   500                         10
2017-04-01T16:00:00Z  56.234.163.24  /welcome   200                         60
2017-04-01T16:00:01Z  56.234.163.24  /welcome   200                        120
2017-04-01T16:00:00Z  56.234.163.24  /welcome   500                         20
2017-04-01T16:00:00Z  56.234.163.23                                                 0.15
2017-04-01T16:00:00Z  56.234.163.24                                                 2.25
====================  =============  ========   ===============  =============  ========
