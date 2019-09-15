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

package org.apache.spark.sql.iceberg.planning

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.iceberg.Config.columnDependencies
import org.apache.spark.sql.iceberg.utils.{Transforms, TypeConversions, currentSparkSession}

import scala.collection.mutable.ArrayBuffer

object IcebergTableValidationChecks {

  private def validationRules(table : CatalogTable,
                              errs : ArrayBuffer[String]) : Unit = {
    val typMapping = TypeConversions.convertStructType(table.schema)

    if (table.bucketSpec.isDefined) {
      errs += "not support for bucketed table"
    }

    if (table.partitionColumnNames.isEmpty) {
      errs += "no support for non partitioned table"
    }

    for (partMappingStr <- columnDependencies(table)) {
      Transforms.fromOption(partMappingStr, typMapping)(currentSparkSession) match {
        case Left(tErrs) => errs ++= tErrs
        case _ => ()
      }
    }
  }

  def validate(action : String,
               table : CatalogTable,
               errs : ArrayBuffer[String] = ArrayBuffer[String]()) : Unit = {
    validationRules(table, errs)

    if (errs.nonEmpty) {
      throw new AnalysisException(
        s"""Cannot ${action} ${table.qualifiedName} as a managed table:
           |${errs.mkString("\n\t", "\n\t", "\n")})""".stripMargin
      )
    }
  }


}
