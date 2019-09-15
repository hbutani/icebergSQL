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

package org.apache.spark.sql.iceberg

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

object Config {

  /**
   * Use to specify that the system should provide additional management capabilities
    * for a table.
   */
  val ADD_MANAGEMENT = "addTableManagement"
  val DEFAULT_ADD_MANAGEMENT = "false"


  private def getOption(tblDesc : CatalogTable,
                        optionName : String) : Option[String] = {

    def get(m : Map[String, String]) : Option[String] = {
      m.get(optionName).orElse(CaseInsensitiveMap(m).get(optionName))
    }

    get(tblDesc.storage.properties).orElse(get(tblDesc.properties))

  }

  def provideManagement(tblDesc : CatalogTable) : Boolean = {
    getOption(tblDesc, ADD_MANAGEMENT).
      getOrElse(DEFAULT_ADD_MANAGEMENT).toBoolean
  }

  def isManaged(tblDesc : CatalogTable) : Boolean = provideManagement(tblDesc)


  /**
    * For tables `addTableManagement=true` use this option to specify
   * column dependencies. This specified as a comma-sperated list of comma
   * separated list of column dependence. A 'column dependence' is of the form
   * `srcCol=destCol:transformFn`, for example
   * `date_col=day_col:extract[2]` where `date_col` is a string in the form `DD-MM-YYYY`.
   * Semantically a column dependence implies that the destCol value can be determined
   * from a srcCol value; the columns are in a one-to-one` or `many-to-one` relationship.
   * The src and dest columns can be any column (data or partition columns) of the table.
   *
   * Currently we support [[Transforms][Iceberg Transforms]] as mapping functions,
   * they must be a string representation of an [[Transforms][Iceberg Transforms]].
   * So users can relate columns based on `date` or `timestamp` elements,
   * based on `truncating` values or `value buckets`.
   *
   * During a table scan we will attempt to transform a predicate on the `srcCol`
   * into a predicate on the `destCol`. For example `date_col='09-12-2019'` will be transformed
   * into `day_col='09'` and applied. If the `destCol` is a partition column
   * this can lead to partition pruning. For example if the table is partitioned by
   * `day_col` then from a predicate `date_col='09-12-2019'` the  inferred predicate
   * `day_col='09'` will lead to only partitions from the 9th day of each month to
   * be scanned. In case the `destCol` is a data column the inferred predicate can  lead
   * to datafiles being pruned based on the statistics available on the column.
   */

  val COLUMN_DEPENDENCIES = "columnDependencies"

  def columnDependencies(tblDesc : CatalogTable) : Option[String] = {
    getOption(tblDesc, COLUMN_DEPENDENCIES)
  }

}
