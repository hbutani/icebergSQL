-- input table
create table if not exists store_sales
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
      path '/Users/hbutani/sparkline/icebergSQL/src/test/resources/store_sales'
    )
    partitioned by (ss_sold_date_sk)
;

msck repair table store_sales;

-- managed table
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
      path '/tmp/store_sales_out',
      addTableManagement "true",
      columnDependencies "ss_sold_date_sk=ss_sold_month:truncate[2],ss_sold_date_sk=ss_sold_day:truncate[4]"
    )
    partitioned by (ss_sold_date_sk)
;

-- first insert
insert into  store_sales_out
  select  *  from store_sales;

-- show snapshots
select * from `store_sales_out$snapshots`;

-- count is 2109
select count(*) from store_sales_out;

-- query on  ss_sold_date_sk='0906245'
select count(*)
from store_sales_out
where ss_sold_date_sk='0906245';

--  insert data gain
insert into  store_sales_out
  select  *  from store_sales;

-- observe 2 snapshots
select * from `store_sales_out$snapshots`;

-- table now has twice number of rows
select count(*) from store_sales_out;

-- query as of first snapshot
-- 1568658741390 is some time between 2 snapshots
-- observe 2109 rows
as of '1568658741390'
select count(*) from store_sales_out;

-- now do an insert overwrite
insert overwrite table  store_sales_out
select  *  from store_sales;

-- observe 3 snapshots
-- latest snapshot adds x files and deletes 2x files
select * from `store_sales_out$snapshots`;

-- table now has 2109 rows again
select count(*) from store_sales_out;

-- query as of second snapshot
-- 1568659495070 is some time between 2 snapshots
-- observe 4218 rows
as of '1568659495070'
select count(*) from store_sales_out;

-- SnapShot on an insert overwrite of 1 partition:
insert overwrite table  store_sales_out partition ( ss_sold_date_sk='0906245' )
select ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_sales_price,ss_sold_month,ss_sold_day from store_sales
where ss_sold_date_sk='0906245' ;

-- observe 4 snapshots
select * from `store_sales_out$snapshots`;

-- table now has 2109 rows again
select count(*) from store_sales_out;

-- SnapShot on an insert overwrite of 1 partition, with source predicate: 5 files added, 5 files deleted
insert overwrite table  store_sales_out partition ( ss_sold_date_sk='0905245' )
select ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_sales_price,ss_sold_month,ss_sold_day from store_sales
where ss_sold_date_sk='0905245' and ss_item_sk < 5000;

-- observe 5 snapshots
select * from `store_sales_out$snapshots`;

-- table now has 1877 rows again
select count(*) from store_sales_out;

-- now a query with ss_item_sk > 5000 on ss_sold_date_sk=0905245 should be a null scan
-- observe no new job listed in Spark UI
select * from store_sales_out
where ss_item_sk > 5000 and ss_sold_date_sk='0905245';
