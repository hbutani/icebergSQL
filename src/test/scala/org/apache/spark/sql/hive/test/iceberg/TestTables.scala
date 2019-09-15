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

import java.io.File
import java.nio.file.Files

trait TestTables {
  self : AbstractTest =>

  val data_folder = new File("src/test/resources").getAbsoluteFile
  val data_path = data_folder.getAbsolutePath

  val store_sales_out_folder = new File(data_folder, "store_sales_out")


  def createStoreSalesTable(tName : String,
                            tFolder : String,
                            tableManagement : Boolean = false,
                            tableTransforms : Seq[String] = Seq.empty
                           ) : Unit = {

    val transformOption = if (tableManagement && tableTransforms.nonEmpty) {
      s""",
         |columnDependencies "${tableTransforms.mkString(",")}"
         |""".stripMargin
    } else ""

    TestIcebergHive.sql(
      s"""create table if not exists ${tName}
    (
            ss_sold_time_sk           int,
         |      ss_item_sk                int,
         |      ss_customer_sk            int,
         |      ss_cdemo_sk               int,
         |      ss_hdemo_sk               int,
         |      ss_addr_sk                int,
         |      ss_store_sk               int,
         |      ss_promo_sk               int,
         |      ss_quantity               int,
         |      ss_wholesale_cost         decimal(7,2),
         |      ss_list_price             decimal(7,2),
         |      ss_sales_price            decimal(7,2),
         |      ss_ext_sales_price        decimal(7,2),
         |      ss_sold_month             string,
         |      ss_sold_day               string,
         |      ss_sold_date_sk string
         |
    )
    USING parquet
    OPTIONS (
         |path "$data_path/$tFolder",
         |addTableManagement "${tableManagement}" ${transformOption}
         |)
    partitioned by (ss_sold_date_sk)""".stripMargin
    )
    TestIcebergHive.sql(s"msck repair table ${tName}")
  }

  def setupStoreSalesTables : Unit = {

    delete(store_sales_out_folder)
    Files.createDirectories(store_sales_out_folder.toPath)
    createStoreSalesTable("store_sales_out",
      "store_sales_out",
      true,
      Seq("ss_sold_date_sk=ss_sold_month:truncate[2]", "ss_sold_date_sk=ss_sold_day:truncate[4]")
    )

    createStoreSalesTable("store_sales", "store_sales")
  }


}
