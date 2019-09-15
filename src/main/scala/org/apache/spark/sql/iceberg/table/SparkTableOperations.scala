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

package org.apache.spark.sql.iceberg.table

import java.lang.String.format
import java.util.{Locale, Objects}

import com.netflix.iceberg.exceptions.CommitFailedException
import com.netflix.iceberg.{BaseMetastoreTableOperations, TableMetadata}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.AlterTableSetPropertiesCommand

/**
  * Very similar to `HiveTableOperations` from `iceberg-hive`.
  * The `refresh` step checks that the `catalogTable` is setup as
  * a Iceberg Table.
  * The ''metadata_location'' property points to the latest iceberg metadata file.
  *
  * As part of a  metadata commit an [[AlterTableSetPropertiesCommand]] is issued
  * that updates the ''metadata_location'' property.
  *
  * @param sparkSession
  * @param catalogTable
  */
class SparkTableOperations private[iceberg] (val sparkSession: SparkSession,
                                             val catalogTable: CatalogTable)
  extends BaseMetastoreTableOperations(sparkSession.sparkContext.hadoopConfiguration) with Logging {

  val tableIdentifier = catalogTable.identifier

  val qualTblNm = {
    QualifiedTableName(
      catalogTable.identifier.database.getOrElse(
        sparkSession.sessionState.catalog.getCurrentDatabase
      ),
      catalogTable.identifier.table
    )
  }

  private var metadataLocation : Option[String] = None

  private def setMetadataLocation(l : String) : Unit = {
    metadataLocation = Some(l)
  }

  override def refresh(): TableMetadata = {

    import BaseMetastoreTableOperations.{ICEBERG_TABLE_TYPE_VALUE, METADATA_LOCATION_PROP, TABLE_TYPE_PROP}

    if (!metadataLocation.isDefined) {
      val catTable = catalogTable
      val tableType = catTable.properties.get(TABLE_TYPE_PROP)

      if (!tableType.isDefined || !tableType.get.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE)) {
        throw new IllegalArgumentException(
          format("Invalid tableName, not Iceberg: %s", tableIdentifier.unquotedString)
        )
      }
      metadataLocation = catTable.properties.get(METADATA_LOCATION_PROP)
    }

    if (!metadataLocation.isDefined) {
      throw new IllegalArgumentException(
        format(format("%s is missing %s property",
          tableIdentifier.unquotedString, METADATA_LOCATION_PROP)
        )
      )
    }
    refreshFromMetadataLocation(metadataLocation.get)
    current
  }

  override def commit(base: TableMetadata, metadata: TableMetadata): Unit = {

    import BaseMetastoreTableOperations.{ICEBERG_TABLE_TYPE_VALUE, METADATA_LOCATION_PROP, PREVIOUS_METADATA_LOCATION_PROP, TABLE_TYPE_PROP}

    // if the metadata is already out of date, reject it
    if (base != current) {
      throw new CommitFailedException(format("stale table metadata for %s",
        tableIdentifier.unquotedString))
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      logInfo("Nothing to commit.")
      return
    }

    val newMetadataLocation: String = writeNewMetadata(metadata, currentVersion + 1)

    var threw: Boolean = true
    try {
      val catTable = catalogTable

      val metadataLocation = catTable.properties.getOrElse(METADATA_LOCATION_PROP, null)

      if (!Objects.equals(currentMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
          format("metadataLocation = %s is not same as table metadataLocation %s for %s",
            currentMetadataLocation, metadataLocation, tableIdentifier.unquotedString))
      }

      val currentTimeMillis = System.currentTimeMillis
      var properties: List[(String, String)] = List(
        (TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH)),
        (METADATA_LOCATION_PROP, newMetadataLocation)
      )

      if (currentMetadataLocation != null && !currentMetadataLocation.isEmpty) {
        properties = properties :+ (PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation)
      }

      val alterTableCmd = new AlterTableSetPropertiesCommand(
        tableIdentifier,
        properties.toMap,
        false)
      alterTableCmd.run(sparkSession)
      log.info(s"Updated table $tableIdentifier with new metadata location: ${newMetadataLocation}")

      sparkSession.sessionState.catalog.invalidateCachedTable(qualTblNm)
      setMetadataLocation(newMetadataLocation)

      threw = false
    } finally {
      if (threw) { // if anything went wrong, clean up the uncommitted metadata file
        io.deleteFile(newMetadataLocation)
      }
    }
    requestRefresh()

  }
}

class SparkTableOperationsForCreate private[iceberg] (sparkSession: SparkSession,
                                                      catalogTable: CatalogTable)
extends SparkTableOperations(sparkSession, catalogTable) {

  override def refresh(): TableMetadata = {
    refreshFromMetadataLocation(null)
    current()
  }
}