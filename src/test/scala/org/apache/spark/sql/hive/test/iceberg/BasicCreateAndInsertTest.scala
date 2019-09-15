/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.test.iceberg

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.iceberg.utils
import org.apache.spark.sql.iceberg.utils.TableUtils
import org.apache.spark.sql.types.IntegerType

class BasicCreateAndInsertTest extends AbstractTest {

  def showSnapShots(table : String) : Unit = {
    val snpShotsDF = TableUtils.snapShotsDF(table)(TestIcebergHive.sparkSession)
    snpShotsDF.show(1000, false)
  }

  val allColumns : String = " * "
  val excludeSoldDate : String = "ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk," +
    "ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_quantity,ss_wholesale_cost," +
    "ss_list_price,ss_sales_price,ss_ext_sales_price,ss_sold_month,ss_sold_day"

  def _insertStatement(insClause : String,
                        toTable : String,
                       fromTable : String,
                       partitionPredicate : String,
                       sourcePredciate : String,
                       showSnapShot : Boolean
                      ) : Unit = {

    val partSpec = if (partitionPredicate != null) {
      s"partition ( ${partitionPredicate} )"
    } else ""

    val whereClause = (partitionPredicate, sourcePredciate) match {
      case (null, null) => ""
      case (p, null) => s"where ${p} "
      case (null, s) => s"where ${s} "
      case (p, s) => s"where ${p} and ${s} "
    }

    val selList = if(partitionPredicate != null) {
      excludeSoldDate
    } else allColumns

    val insStat = s"""
                     |${insClause} ${toTable} ${partSpec}
                     |select ${selList} from ${fromTable}
                     |${whereClause}
      """.stripMargin

    println(insStat)

    TestIcebergHive.sql(insStat)
    if (showSnapShot) {
      showSnapShots(toTable)
    }

  }

  def insert(toTable : String,
             fromTable : String,
             partitionPredicate : String = null,
             sourcePredciate : String = null,
             showSnapShot : Boolean = true
            ) : Unit = {
    _insertStatement(
      "insert into ",
      toTable,
      fromTable,
      partitionPredicate,
      sourcePredciate,
      showSnapShot
    )
  }

  def insertOverwrite(toTable : String,
                      fromTable : String,
                      partitionPredicate : String = null,
                      sourcePredciate : String = null,
                      showSnapShot : Boolean = true
                     ) : Unit = {

    _insertStatement(
      "insert overwrite table ",
      toTable,
      fromTable,
      partitionPredicate,
      sourcePredciate,
      showSnapShot
    )

  }

  ignore("a") { td =>

    TestIcebergHive.sql(
      "select distinct ss_item_sk from store_sales order by ss_item_sk"
    ).show(10000, false)

    // item range 16 - 17986

  }

  test("b") { td =>

   val c = Cast(Literal("123"), IntegerType)
    println(c.eval(null))
  }

  test("test1") { td =>

    var df : DataFrame = null

    println("Initially no snapShots:")
    showSnapShots("store_sales_out")

    println("SnapShot on an insert: 30 files added")
    insert("store_sales_out", "store_sales")
    TestIcebergHive.sql("select count(*) from store_sales_out").
      show(10000, false)

    //  a query with predicate ss_sold_date_sk='0906245'
    // has predicate ss_sold_month='09' added
    println("Query with  ss_sold_date_sk='0906245' has predciate ss_sold_month='09' added")
    df = TestIcebergHive.sql(
      "select count(*) from store_sales_out where ss_sold_date_sk='0906245'")
    assert(
      utils.iceTableScanExec(df).get.getIceFilter.toString.
        contains("""ref(name="ss_sold_month") == "09"""")
    )
    df.show(1000, false)

    println("SnapShot on another insert: 30 files added again")
    insert("store_sales_out", "store_sales")

    TestIcebergHive.sql("select count(*) from store_sales_out").
      show(10000, false)

    println("SnapShot on an insert overwrite: 30 files added, 60 files deleted")
    insertOverwrite("store_sales_out", "store_sales")
    TestIcebergHive.sql("select count(*) from store_sales_out").
      show(10000, false)

    println("SnapShot on an insert overwrite of 1 partition: 5 files added, 5 files deleted")
    insertOverwrite("store_sales_out", "store_sales", "ss_sold_date_sk='0906245'")
    TestIcebergHive.sql("select count(*) from store_sales_out").
      show(10000, false)

    println("SnapShot on an insert overwrite of 1 partition, with source predicate:" +
      " 5 files added, 5 files deleted")
    insertOverwrite("store_sales_out", "store_sales",
      "ss_sold_date_sk='0905245'", "ss_item_sk < 5000")
    TestIcebergHive.sql("select count(*) from store_sales_out").
      show(10000, false)

    // now a query with ss_item_sk > 5000 on ss_sold_date_sk=0905245 should be a null scan
    println("now a query with ss_item_sk > 5000 on ss_sold_date_sk=0905245 should be a null scan")
    df = TestIcebergHive.sql(
      "select * from store_sales_out where ss_item_sk > 5000 and ss_sold_date_sk='0905245'")
    assert(utils.filesScanned(df).size == 0)
    df.show(10000, false)
  }

}
