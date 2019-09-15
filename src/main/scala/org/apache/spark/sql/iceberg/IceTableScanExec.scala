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

import com.netflix.iceberg.DataFile
import com.netflix.iceberg.expressions.{ExpressionVisitors, Expression => IceExpression}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.execution.{CodegenSupport, FileSourceScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.iceberg.table.SparkTables
import org.apache.spark.sql.iceberg.utils.DelegationUtils.{DelegatedField, DelegatedMethod0, DelegatedMethod1}
import org.apache.spark.sql.iceberg.utils.{ColumnDependenciesExprVisitor, ExpressionConversions, Transforms}
import com.netflix.iceberg.transforms.{Transform => IceTransform}
import org.apache.spark.sql.iceberg.utils.TypeConversions.TypeMapping

/**
 * Setup as a parent Physical Operator of a [[FileSourceScanExec]]. During execution
 * before handing over control to its child [[FileSourceScanExec]] operator it updates its
 * `selectedPartitions` member.
 *
 * This is computed based on the `partitionFilters` and `dataFilters` associated with this scan.
 * These are converted to an [[IceExpression]], further [[IceExpression]] are added based on
 * `column dependencies` defined for this table. From the current Iceberg snaphot a list of
 * [[DataFile]] are computed. Finally the [[FileSourceScanExec]] list of selected partitions
 * list is updated to remove files from [[PartitionDirectory]] instances not in this list of
 * DataFiles.
 *
 * @param child
 * @param catalogTable
 */
case class IceTableScanExec(child: FileSourceScanExec,
                            @transient catalogTable: CatalogTable
                           )
  extends UnaryExecNode with CodegenSupport {

  @transient private val iceTable = {
    implicit val ss = sqlContext.sparkSession
    val iceTables = new SparkTables(ss, catalogTable)
    val tbId = utils.qualifiedTableIdentifier(catalogTable.identifier, ss)
    iceTables.load(tbId.database.getOrElse(null), tbId.table)
  }

  lazy val sparkExpressionToPushToIce: Expression = {
    val alwaysTrue : Expression = Literal(true)
    (child.partitionFilters ++ child.dataFilters).foldLeft(alwaysTrue)(And)
  }

  @transient private lazy val icebergFilter: IceExpression = {
    val iceExpr = ExpressionConversions.convert(sparkExpressionToPushToIce)
    columnDependencies.map { cds =>
      val exprVisitor = new ColumnDependenciesExprVisitor(iceTable.schema().asStruct(), cds)
      ExpressionVisitors.visit(iceExpr, exprVisitor)
    }.getOrElse(iceExpr)
  }

  @transient private lazy val iceDataFiles : Seq[DataFile] = {
    val iceScan = iceTable.newScan().filter(icebergFilter)
    import scala.collection.JavaConversions._
    iceScan.planFiles().map(_.file).toSeq
  }

  @transient private val selectedPartitionsDelegate =
    new DelegatedField[Seq[PartitionDirectory], FileSourceScanExec, FileSourceScanExec](
      child, "org$apache$spark$sql$execution$FileSourceScanExec$$selectedPartitions")

  private val doExecute_d =
    new DelegatedMethod0[SparkPlan, FileSourceScanExec, RDD[InternalRow]](child, "doExecute")

  private val doProduce_d =
    new DelegatedMethod1[CodegenSupport, FileSourceScanExec, String, CodegenContext](
      child, "doProduce")


  private var updateSelectedPartitionsDone : Boolean = false

  private def updateSelectedPartitions : Unit = {
    if (!updateSelectedPartitionsDone) {
      child.metadata // trigger construction of child selectedPartitions

      val dataFiles = iceDataFiles
      val dFilePaths = dataFiles.map(_.path().toString).toSet
      val sParts = selectedPartitionsDelegate.value

      val  updtSelParts = sParts.map { sPart =>
        PartitionDirectory(
          sPart.values,
          sPart.files.filter(fs => dFilePaths.contains(fs.getPath.toString))
        )
      }
      selectedPartitionsDelegate.value = updtSelParts
      updateSelectedPartitionsDone = true
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    updateSelectedPartitions
    child.inputRDDs()
  }

  override protected def doExecute(): RDD[InternalRow] = {
    updateSelectedPartitions
    doExecute_d.apply
  }

  override def output: Seq[Attribute] = child.output

  override protected def doProduce(ctx: CodegenContext): String = {
    updateSelectedPartitions
    child.produce(ctx, this)
  }

  override  def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    updateSelectedPartitions
    parent.doConsume(ctx, input, row)
  }

  override lazy val (outputPartitioning, outputOrdering) : (Partitioning, Seq[SortOrder]) =
    (child.outputPartitioning, child.outputOrdering)

  override def doCanonicalize(): IceTableScanExec = {
    IceTableScanExec(child.doCanonicalize(), catalogTable)
  }

  private def columnDependencies : Option[Map[String, Map[String, IceTransform[_,_]]]] = {
    Config.columnDependencies(catalogTable).flatMap { colDeps =>
      val typMapping = TypeMapping(catalogTable.schema, iceTable.schema().asStruct())
      val t = Transforms.fromOption(colDeps, typMapping)(sqlContext.sparkSession)
      t.right.toOption
    }
  }

  // for testing only
  def showPartitionsScanned : Seq[PartitionDirectory] = {
    updateSelectedPartitions
    selectedPartitionsDelegate.value
  }

  // for testing only
  def getIceFilter = icebergFilter

}
