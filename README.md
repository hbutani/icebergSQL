# Integrating Iceberg Table Management into Spark SQL

[Iceberg]({https://iceberg.apache.org/spec/) introduces the
concept of *Table formats* (as opposed to File Formats) that defines an access
and management model for Tables in Big Data systems. At its
core it is a well documented and portable specification of versionable Table
metadata (both physical and logical metadata). On top of this it provides a set
of capabilities: Snapshot isolation, significant speed and simplicity in access
of  Table metadata critical for Query Planning overhead( even in the case of datasets with millions
of files and partitions), schema evolution, and partition layout isolation( hence
the ability to change physical layout without changing Applications). 

These capabilities fill some very critical gaps of
Table management in Big Data systems, and hence various open source communities
have quickly adopted/integrated Iceberg functionality. Iceberg was initially developed
at Netflix; subsequently(likely because of its wide appeal) Netflix has
graciously incubated it as an [Apache project](https://github.com/apache/incubator-iceberg).

It does this by defining clear contracts for underlying file formats such
as  how schema and statistics from these formats are available to/in Iceberg. It
prescribes how Iceberg capabilities can be integrated into existing Big Data
scenarios through various packaged components such as *iceberg-parquet*, *iceberg-orc*,
*iceberg-avro* for applications that directly manage *parquet, orc and avro*
files. Further *iceberg-hive*, *iceberg-presto* and *iceberg-spark* are packaged jars
that can be dropped into scenarios using /hive, presto and spark/ and want to leverage
Iceberg to manage datasets. 

[Apache Spark]() is rapidly gaining traction as a platform for Enterprise Analytical
workloads: for example our [Oracle SNAP Spark native OLAP Platform](https://tinyurl.com/y8hbyp9q) 
( [see also](https://tinyurl.com/y59k23bf)) is used  by a Fortune 10 company to
power their Finance Data Lake. These tend to be **SQL heavy** (in fact almost
exclusively SQL based) solutions. It is a time honored tradition to surface
analytical and management functionality in SQL, for example as SQL Row and Table
functions, or Database options like OLAP and Geospatial capabilities.
Data Management is a critical aspect of an
Analytical platform, but unfortunately is an underdeveloped component of Apache
Spark. This has led customers to come up with their own data management schemes 
such as using Hive ACID Tables for data management with
Spark for query processing, or custom solutions using ETL platforms and tools
such as Talend and Airflow. Providing Table management that is seamlessly
integrated into familiar SQL verbs such as *create table*, *insert*, and *select*
simplifies the task of developing Analytical solutions on Apache Spark and
will drive further adoption.

For [Apache Spark](), Iceberg integration is not fully available for the SQL layer. 
There is work going on to surface 
[Iceberg Table Management as a V2 Datasource table](https://databricks.com/session/apache-spark-data-source-v2), 
but V2 Datasources itself are [not fully integrated into Spark SQL](https://tinyurl.com/y5u576gk) 
( [see also]({https://tinyurl.com/yylna72p)).  Given the significant of Apache Spark 2.x we feel it is useful
to provide the Table Management for Datasource V1 tables, bringing this
functionality to a large deployed base.  These reasons led us to develop the ability to use 
Iceberg Table Management capabilities with Spark SQL, specifically Datasource V1 tables. Our component will:
- allow users to **create managed tables** and define source column to partition
  column transformations as table options.  
- have **SQL insert statements create new Iceberg Table snapshots**
- have **SQL select~ statements leverage Iceberg Table snapshots** for partition
  and file pruning
- provide **a new 'as of' clause to the sql select statement** to run a query against a
  particular snapshot of a managed table.
- **extend Spark SQL with Iceberg management views and statements** to view and manage the
  snapshots of a managed table.

## How to use the functionality?

- setup spark.sql.extensions= org.apache.spark.sql.iceberg.planning.SparkSessionExtensions
- setup classpath via
  - spark.executor.extraClassPath + spark.driver.extraClassPath
  - OR
  - spark.jars

## A detailed example

### Table creation
Consider the following definition of a `store_sales_out` table. 
```sql
create table if not exists store_sales_out
    (
      ss_sold_time_sk           int,
      ss_item_sk                int,
      ss_customer_sk            int,
      ss_cdemo_sk               int,
      ss_hdemo_sk               int,
      ss_addr_sk                int,
      ss_store_sk               int,
      ss_promo_sk               int,
      ss_quantity               int,
      ss_wholesale_cost         decimal(7,2),
      ss_list_price             decimal(7,2),
      ss_sales_price            decimal(7,2),
      ss_ext_sales_price        decimal(7,2),
      ss_sold_month             string,
      ss_sold_day               string,
      ss_sold_date_sk string
    )
    USING parquet
    OPTIONS (
      path "src/test/resources/store_sales",
      addTableManagement "true",
      columnDependencies "ss_sold_date_sk=ss_sold_month:truncate[2],ss_sold_date_sk=ss_sold_day:truncate[4]"
    )
    partitioned by (ss_sold_date_sk)
```

This is regular Spark datasources v1 table that is partitioned on `ss_sold_date_sk`.
It is defined with two extra options. Setting The **addTableManagement** to true wil make this a table 
that will be integrated with the Table management infratstructure from *Iceberg*. The 
**columnDependencies** can be used to defined functional dependencies between table columns in terms
of *Iceberg Transforms*; these will be used for partition and datafile pruning during query planning.
More on when we talk about querying.

Assume there is another `store_sales` table with the same schema that is used to 
insert/update the `store_sales_out table`. This entire example is in the
 _BasicCreateAndInsertTest_ test class. We refer you to this class for the DDL for the `store_sales` table and 
 other details in this example.
 
Initially the `store_sales_out` has no snapshots as can be seen from the output of the `showSnapShots` invocation
in _BasicCreateAndInsertTest_. We will be providing a `snapshots_view` shortly, so users will be able to
issue a `select * from <table_name>$snapshots`(this is similar to `snap$` views we have in our SNAP product).
The table identifier has the form <table_name> followed by the string `$snapshots_view`. So for `store_sales_out` 
you would have to issue a `select * from store_sales_out$snapshots_view`.

```
Initially no snapShots:
select * from `store_sales_out$snapshots`
+---+--------+----------+-------------+----------------+--------------------+
|id |parentId|timeMillis|numAddedFiles|numdDeletedFiles|manifestListLocation|
+---+--------+----------+-------------+----------------+--------------------+
+---+--------+----------+-------------+----------------+--------------------+
```

### Insert into store_sales_out without any partition specification.
Let's insert into the `store_sales_out` table. So we issue
```sql
insert into  store_sales_out 
  select  *  from store_sales
```

This creates a SnapShot with 30 files added. The `store_sales` table has `6` partitions with 5 files in each partition.
```
select * from `store_sales_out$snapshots`

+-------------------+--------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|id                 |parentId|timeMillis   |numAddedFiles|numdDeletedFiles|manifestListLocation                                                                                                                                |
+-------------------+--------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|8311655904283006343|-1      |1566875511640|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-8311655904283006343-1-51cd6794-8cc1-4433-8055-d268dbe62202.avro|
+-------------------+--------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
```

The table now has 2109 rows.
```
select count(*) from store_sales_out;

+--------+
|count(1)|
+--------+
|2109    |
+--------+
```

### Query with  ss_sold_date_sk='0906245' has predciate ss_sold_month='09' added
Running a query with a partition filter
```sql
select count(*) 
from store_sales_out 
where ss_sold_date_sk='0906245'
```

We have defined `ss_sold_month` is related to `ss_sold_date_sk` via a `truncate` transformation.
So for this query the `ss_sold_month='09'` is pushed to the _TableScan_ Iceberg operation.
This is observed by introspecting the `icebergFilter` property of the _IceTableScanExec_ physical
operator.

The output shows there are 236 rows in this partition.
```
+--------+
|count(1)|
+--------+
|236     |
+--------+
```

### Issue another Insert into store_sales_out without any partition specification.

Let's issue another insert into the `store_sales_out` table. 
```sql
insert into  store_sales_out 
  select  *  from store_sales
```
This creates another SnapShot with 30 files added.
```
select * from `store_sales_out$snapshots`

+-------------------+------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|id                 |parentId          |timeMillis   |numAddedFiles|numdDeletedFiles|manifestListLocation                                                                                                                                |
+-------------------+------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|369432757121624247 |-1                |1566958072042|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-369432757121624247-1-24288117-f3b9-4f85-aa94-b943cabd844d.avro |
|2542920950855973853|369432757121624247|1566958075214|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-2542920950855973853-1-39abbb75-1022-4ca2-b274-1ea10f445a9b.avro|
+-------------------+------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
```

The table now has 4218 rows.
```
select count(*) from store_sales_out;

+--------+
|count(1)|
+--------+
|4218    |
+--------+
```

### Run select as of first insert
If we query the table as of the first insert we still see `2109` rows
```sql
as of '2019-09-15 20:32:24.062'
select count(*) from store_sales_out

+--------+
|count(1)|
+--------+
|2109    |
+--------+

```

### Issue an Insert Overwrite into store_sales_out without any partition specification.

Now let's issue a insert overwrite on the `store_sales_out` table. 
```sql
insert overwrite table  store_sales_out 
  select  *  from store_sales
```

This creates a SnapShot with 30 files added and 60 files deleted.
```

select * from `store_sales_out$snapshots`

+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|id                 |parentId           |timeMillis   |numAddedFiles|numdDeletedFiles|manifestListLocation                                                                                                                                |
+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|369432757121624247 |-1                 |1566958072042|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-369432757121624247-1-24288117-f3b9-4f85-aa94-b943cabd844d.avro |
|2542920950855973853|369432757121624247 |1566958075214|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-2542920950855973853-1-39abbb75-1022-4ca2-b274-1ea10f445a9b.avro|
|6277089168341855684|2542920950855973853|1566958077282|30           |60              |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-6277089168341855684-1-99eeb5d3-7b1b-4c54-ba3e-2cb1d6946cbe.avro|
+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
```

The table again has 2109 rows.
```
select count(*) from store_sales_out;

+--------+
|count(1)|
+--------+
|2109    |
+--------+
```

### Insert overwrite of 1 partition

Next we insert overwrite 1 partition of store_sales_out

```sql
insert overwrite table  store_sales_out partition ( ss_sold_date_sk='0906245' )
  select ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,
       ss_store_sk,ss_promo_sk,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,
       ss_ext_sales_price,ss_sold_month,ss_sold_day from store_sales
  where ss_sold_date_sk='0906245' 

```

This creates a SnapShot with 5 files added and 5 files deleted.
```

select * from `store_sales_out$snapshots`

+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|id                 |parentId           |timeMillis   |numAddedFiles|numdDeletedFiles|manifestListLocation                                                                                                                                |
+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|369432757121624247 |-1                 |1566958072042|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-369432757121624247-1-24288117-f3b9-4f85-aa94-b943cabd844d.avro |
|2542920950855973853|369432757121624247 |1566958075214|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-2542920950855973853-1-39abbb75-1022-4ca2-b274-1ea10f445a9b.avro|
|6277089168341855684|2542920950855973853|1566958077282|30           |60              |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-6277089168341855684-1-99eeb5d3-7b1b-4c54-ba3e-2cb1d6946cbe.avro|
|4984732539170247398|6277089168341855684|1566958078575|5            |5               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-4984732539170247398-1-00a53e56-fd8f-4293-9554-41f2b89ae2d2.avro|
+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
```

The table still has 2109 rows.
```
select count(*) from store_sales_out;

+--------+
|count(1)|
+--------+
|2109    |
+--------+
```

### Insert overwrite of 1 partition with predicates on the source data
We run an insert overwrite on 1 partition again, but now we have an extra predicate on source rows
that reduces the number of rows inserted into this partition.

```sql
insert overwrite table  store_sales_out partition ( ss_sold_date_sk='0905245' )
  select ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,
         ss_addr_sk,ss_store_sk,ss_promo_sk,ss_quantity,ss_wholesale_cost,ss_list_price,
         ss_sales_price,ss_ext_sales_price,ss_sold_month,ss_sold_day from store_sales
  where ss_sold_date_sk='0905245' and ss_item_sk < 5000 
```

This creates a SnapShot with 5 files added and 5 files deleted. 
```

select * from `store_sales_out$snapshots`

+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|id                 |parentId           |timeMillis   |numAddedFiles|numdDeletedFiles|manifestListLocation                                                                                                                                |
+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|369432757121624247 |-1                 |1566958072042|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-369432757121624247-1-24288117-f3b9-4f85-aa94-b943cabd844d.avro |
|2542920950855973853|369432757121624247 |1566958075214|30           |0               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-2542920950855973853-1-39abbb75-1022-4ca2-b274-1ea10f445a9b.avro|
|6277089168341855684|2542920950855973853|1566958077282|30           |60              |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-6277089168341855684-1-99eeb5d3-7b1b-4c54-ba3e-2cb1d6946cbe.avro|
|4984732539170247398|6277089168341855684|1566958078575|5            |5               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-4984732539170247398-1-00a53e56-fd8f-4293-9554-41f2b89ae2d2.avro|
|713868995008319946 |4984732539170247398|1566958079727|5            |5               |/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales_out/metadata/snap-713868995008319946-1-2ce5fb56-906d-4a66-a27f-2b5fad668560.avro |
+-------------------+-------------------+-------------+-------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
```

But the table count has reduced because of the extra source predciate.

```
select count(*) from store_sales_out;

+--------+
|count(1)|
+--------+
|1877    |
+--------+

```

### Run select as of first insert
If we can still query the table as of the first insert and we will get `2109` rows
```sql
as of '2019-09-15 20:32:24.062'
select count(*) from store_sales_out

+--------+
|count(1)|
+--------+
|2109    |
+--------+

```

### Query the last insrted partition with the invert of the predicate used in the last insert

```sql
select * from store_sales_out 
where ss_item_sk > 5000 and ss_sold_date_sk='0905245'
```

We used the predicate `ss_item_sk < 5000` fot the insert and here we use its invert `ss_item_sk > 5000`
We can validate that this is a _NullScan_ by observing that zero files are scanned by the 
_IceTableScanExec_ physical operator

## Under the covers

See this [design note](docs/icebergSparkSQL.pdf) for more details. Note that is out-dated in some
of the details, when there are discrepenancies in detailed feature descriptions than this page is more
current. But the note still provides a decent picture of how the integration works under the covers. 

A `SparkSessionExtensions` is setup with Planning and Parsing extensions. As explained in the 
*How to use* section set the `spark.sql.extensions` to this so these rules and extensions are 
in effect in the _SparkSessions_ created in the deployed Spark Context.

### Table Creation
The _CreateIcebergTableRule_ checks that tables marked as `addTableManagement=true` are
supported. Currently we require the table to be non-bucketed and partitioned
(we plan to relax the partitioning constraint soon); and the table columns must be of 
*Atomic, Map, Array, or Struct type*. 
 
### The Column Dependencies option
If `columnDependencies` option is specified then this
must be in the form of a comma separated list of column dependence. 
A 'column dependence' is of the form `srcCol=destCol:transformFn`, for example
`date_col=day_col:extract[2]` where `date_col` is a string in the form `DD-MM-YYYY`.
Semantically a column dependence implies that the destCol value can be determined
from a srcCol value; the columns are in a one-to-one` or `many-to-one` relationship.
The src and dest columns can be any column (data or partition columns) of the table.

Currently we support *Iceberg Transforms* as mapping functions.
So users can relate columns based on `date` or `timestamp` elements,
based on `truncating` values or `value buckets`.

During a table scan we will attempt to transform a predicate on the `srcCol`
into a predicate on the `destCol`. For example `date_col='09-12-2019'` will be transformed
into `day_col='09'` and applied. If the `destCol` is a partition column
this can lead to partition pruning. For example if the table is partitioned by
`day_col` then from a predicate `date_col='09-12-2019'` the  inferred predicate
`day_col='09'` will lead to only partitions from the 9th day of each month to
be scanned. In case the `destCol` is a data column the inferred predicate can  lead
to datafiles being pruned based on the statistics available on the column.

### Inserting into a snapshot managed table

Spark Insert Plans involve 3 major components:  the
_InsertHadoopFsRelation_ Spark Command, the _FileFormat Writer_ and the 
_File Commit Protocol_. Table metadata information(up to the granularity of table
partitions) is retrieved and updated from the Spark Catalog, whereas File information and 
interaction is done via the _File System_ API. 

- **InsertHadoopFsRelation:** orchestrates the entire operation, it also handles
  interaction with the Spark Catalog. It's logic executes in the Driver of
  the SparkContext. The actions it performs are: compute the affected 
  partitions based on the /partition specification/ in the Insert statement, 
  setup the File Commit Protocol and the Write Job that is associated with 
  the File Format Writer, execute and commit/abort the job, compute the set 
  of Added and Deleted partitions, and update the Spark Catalog.
- **File Commit Protocol:** tracks changes to data files made by the job and
  provides rudimentary level of job isolation. It provides a set of 
  callbacks like new Task file, Task commit/abort and Job commit/abort that
  the other components use to notify it of file changes. On commit
  it moves files into their final locations, after which other operations
  will see the new list of the Table files.
- **Write Tasks:** create and write new Files, notify the File Commit
  Protocol of new files.
  
See  **Figure: Spark Insert Command execution** in the design for a detail 
sequence diagram of how a insert is executed. For snapshot managed tables
we replace the ~InsertHadoopFsRelation~ Command with an 
~Insert Into IcebergTable~ command.

#### Insert Into IcebergTable command
This a drop-in replacement for InsertIntoHadoopFsRelationCommand setup by the 
~IcebergTableWriteRule~. By and large follows the same execution flow as
~InsertIntoHadoopFsRelation~ Command with the following behavior overrides.

- The write must be on a CatalogTable. So catalogTable parameter is not optional.
- Since this is a iceberg managed table we load the IceTable metadata for this table.
- `initialMatchingPartitions` is computed from the IceTable metadata
- since data files must be managed by iceberg custom partition
  locations cannot be configured for this table.
- an `IcebergFileCommitProtocol` is setup that wraps the underlying
  FileCommitProtocol. This mostly defers to the underlying commitProtocol
  instance; in the process it ensures iceberg DataFile instances are created for
  new files on task commit which are then delivered to the Driver
  `IcebergFileCommitProtocol` instance via `TaskCommitMessages`.
 - The underlying `FileCommitProtocol` is setup with `dynamicPartitionOverwrite`
   mode set to false. Since IceTable metadata is used by scan operations to
   compute what files to scan we don't have to do an all-or-nothing replacement
   of files in a partition that is needed for dynamic partition mode using the
   FileCommitProtocol.
- in case of dynamicPartitionOverwrite mode we don't clear specified source
  Partitions, because we want the current files to be able execute queries
  against older snapshots.
- once the job finishes the Catalog is updated with 'new' and 'deleted'
  partitions just as it is in a regular InsertIntoHadoopFsRelationCommand
- then based on the 'initial set' of DataFile and the set of DataFile created by
  tasks of this job a new iceberg Snapshot is created.
- finally cache invalidation and stats update actions happen just like in a
  regular InsertIntoHadoopFsRelationCommand.

#### Iceberg File Commit Protocol

Provides the following function on top of the 'normal' Commit Protocol. Commit
actions are simply deferred to the 'designate' except in the following: 

- track files created for each Task in a TaskPaths instance. This tracks the
  temporary file location and also the location that the file will be moved to
  on a commit. 
- on Task Commit build an Iceberg DataFile instance. Currently only if the file
  is a parquet file we will also build column level stats.
  - The TaskCommitMessage we send back has a payload of
    IcebergTaskCommitMessage, which encapsulates  the TaskCommitMessage build by
    the 'designate' and the DataFile instances. 
- we ignore deleteWithJob invocations, as we want to keep historical files
  around. These will be removed via a clear snapshot command. 
- on a commitJob we extract all the DataFile instances from the
  IcebergTaskCommitMessage messages and expose a addedDataFiles list which is
  used by IceTableScanExec to build the new Iceberg Table Snapshot. 
  

### Scanning a snapshot managed table
This is handled by a ~Iceberg Table Scan~ physical operator. 
This is setup as a **parent** Physical Operator of a [[FileSourceScanExec]]. During execution
before handing over control to its child [[FileSourceScanExec]] operator it updates its `selectedPartitions` 
member.

This is computed based on the `partitionFilters` and `dataFilters` associated with this scan.
These are converted to an [[IceExpression]], further [[IceExpression]] are added based on
`column dependencies` defined for this table. From the current Iceberg snaphot a list of
[[DataFile]] are computed. Finally the [[FileSourceScanExec]] list of selected partitions
list is updated to remove files from [[PartitionDirectory]] instances not in this list of
DataFiles.
