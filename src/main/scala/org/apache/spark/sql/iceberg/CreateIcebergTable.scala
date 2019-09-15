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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand, DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.iceberg.table.SparkTablesForCreate
import org.apache.spark.sql.iceberg.utils.{SchemaConversions, _}
import org.apache.spark.sql.{Row, SparkSession}

trait IcebergTableCreation {
  val catalogTable: CatalogTable

  def createIcebergTable(sparkSession: SparkSession) : Unit = {
    implicit val ss = currentSparkSession
    import scala.collection.JavaConversions._
    val (schema, partSpec) = SchemaConversions.toIcebergSpec(catalogTable)
    val iceTables = new SparkTablesForCreate(ss, catalogTable)
    val tId = parseIdentifier(catalogTable.qualifiedName, ss)
    iceTables.create(schema, partSpec, Map.empty[String,String], catalogTable.qualifiedName)
  }
}

case class CreateIcebergTable(createTblCmd : CreateDataSourceTableCommand)
  extends RunnableCommand with IcebergTableCreation {

  val catalogTable = createTblCmd.table

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val rows = createTblCmd.run(sparkSession)
    createIcebergTable(sparkSession)
    rows
  }
}


case class CreateIcebergTableAsSelect(createTblCmd : CreateDataSourceTableAsSelectCommand)
  extends DataWritingCommand with IcebergTableCreation {

  override def query: LogicalPlan = createTblCmd.query

  override def outputColumnNames: Seq[String] = createTblCmd.outputColumnNames

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val rows = createTblCmd.run(sparkSession, child)
    createIcebergTable(sparkSession)
    rows
  }

  override val catalogTable: CatalogTable = createTblCmd.table
}