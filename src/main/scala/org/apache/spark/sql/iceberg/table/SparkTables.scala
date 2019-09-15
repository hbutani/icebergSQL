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

import java.util

import com.netflix.iceberg.TableMetadata.newTableMetadata
import com.netflix.iceberg.{BaseMetastoreTableOperations, BaseMetastoreTables, BaseTable, PartitionSpec, Schema, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.iceberg.utils._

/**
  * Very similar to `HiveTables` from `iceberg-hive`.
  * Setups a [[SparkTableOperations]], `tableIdentifier` is interpreted using the session parser.
  *
  * Uses of this should pass the Spark table's [[CatalogTable]] on which Table operations will be
  * applied and the current [[SparkSession]].
  *
  * @param sparkSession
  * @param catalogTable
  */
private[iceberg] class SparkTables(val sparkSession: SparkSession,
                                   val catalogTable: CatalogTable)
  extends BaseMetastoreTables(sparkSession.sparkContext.hadoopConfiguration) {

  override def newTableOps(configuration: Configuration,
                           database: String,
                           table: String
                          ): BaseMetastoreTableOperations = {
    new SparkTableOperations(sparkSession, catalogTable)
  }

  override def create(schema: Schema,
                      partitionSpec: PartitionSpec,
                      map: util.Map[String, String],
                      tableIdentifier: String
                     ): Table = {
    val tId = parseIdentifier(tableIdentifier, sparkSession)
    create(schema, partitionSpec, map, tId.database.getOrElse(null), tId.table)
  }

  override def create(schema: Schema, spec: PartitionSpec,
                      properties: util.Map[String, String],
                      database: String,
                      table: String
                     ): Table = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val ops = newTableOps(conf, database, table)
    val location = catalogTable.storage.locationUri.map(CatalogUtils.URIToString(_)).get
    val metadata = newTableMetadata(ops, schema, spec, location, properties)
    ops.commit(null, metadata)
    new BaseTable(ops, database + "." + table)
  }

  override def load(tableIdentifier: String): Table = {
    val tId = parseIdentifier(tableIdentifier, sparkSession)
    val qtid = qualifiedTableIdentifier(tId, sparkSession)
    load(qtid.database.getOrElse(null), qtid.table)
  }
}

private[iceberg] class SparkTablesForCreate(sparkSession: SparkSession,
                                            catalogTable: CatalogTable)
  extends SparkTables(sparkSession, catalogTable) {

  override def newTableOps(configuration: Configuration,
                           database: String,
                           table: String
                          ): BaseMetastoreTableOperations = {
    new SparkTableOperationsForCreate(sparkSession, catalogTable)
  }

}
