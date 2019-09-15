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

package org.apache.spark.sql.iceberg.utils

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.iceberg.table.SparkTables
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TableUtils {

  protected[this] def formatName(name: String)(implicit ss : SparkSession) : String = {
    if (ss.sqlContext.conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  def iceTable(tblNm : String)(implicit ss : SparkSession) : IceTable = {
    val name = parseIdentifier(tblNm, ss)

    val db = formatName(name.database.getOrElse(ss.sessionState.catalog.getCurrentDatabase))
    val table = formatName(name.table)
    val catalogTable = ss.sessionState.catalog.externalCatalog.getTable(db, table)
    val iceTables = new SparkTables(ss, catalogTable)
    val tbId = qualifiedTableIdentifier(catalogTable.identifier, ss)
    iceTables.load(tbId.database.getOrElse(null), tbId.table)
  }

  val SNAPSHOT_SCHEMA : StructType = {
    StructType(
      Seq(
        StructField("id", LongType),
        StructField("parentId", LongType),
        StructField("timeMillis", LongType),
        StructField("numAddedFiles", IntegerType),
        StructField("numdDeletedFiles", IntegerType),
        StructField("manifestListLocation", StringType)
      )
    )
  }

  def snapShots(iceTable : IceTable)(implicit ss : SparkSession) : Seq[Row] = {
    import scala.collection.JavaConversions._
    val rows = for (sShot <- iceTable.snapshots()) yield {

      val pId : Long = {
        val l = sShot.parentId()
        if (l == null) -1 else l
      }

      new GenericRow(
        Array[Any](
          sShot.snapshotId(),
          pId,
          sShot.timestampMillis(),
          sShot.addedFiles().size,
          sShot.deletedFiles().size,
          sShot.manifestListLocation()
        )
      )
    }
    rows.toSeq
  }

  def snapShotsDF(iceTable : IceTable)(implicit ss : SparkSession) : DataFrame =
    dataframe(snapShots(iceTable), SNAPSHOT_SCHEMA)

  def snapShotsDF(tblNm : String)(implicit ss : SparkSession) : DataFrame = {
    val iTbl = iceTable(tblNm)
    dataframe(snapShots(iTbl), SNAPSHOT_SCHEMA)
  }
}
