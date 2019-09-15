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

import com.netflix.iceberg.{DataFile, PendingUpdate, Snapshot}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, CommandUtils, DataWritingCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.iceberg.{PartitioningUtils => IcePartitioningUtils}
import org.apache.spark.sql.iceberg.table.SparkTables
import org.apache.spark.sql.iceberg.utils.{DataFileUtils, ExpressionConversions, IceTable}
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}

/**
 * A drop-in replacement for [[InsertIntoHadoopFsRelationCommand]] setup by the
 * [[org.apache.spark.sql.iceberg.planning.IcebergTableWriteRule]]. By and large
 * follows the same execution flow as [[InsertIntoHadoopFsRelationCommand]] with the
 * following behavior overrides.
 *
 *  - The write must be on a [[CatalogTable]]. So `catalogTable` parameter is not optional.
 *  - Since this is a `iceberg managed table` we load the [[IceTable]] metadata for this table.
 *  - `initialMatchingPartitions` is computed from the [[IceTable]] metadata; see method
 *    `matchIceDataFiles`
 *  - since data files must be managed by iceberg we require that ''custom partition locations''
 *    are not configured for this table.
 *  - we setup an [[IcebergFileCommitProtocol]] that wraps the underlying [[FileCommitProtocol]].
 *    The  [[IcebergFileCommitProtocol]] mostly defers to the underlying commitProtocol instance;
 *    in the process it ensures iceberg [[DataFile]] instances are created for new  files on
 *    task commit which are then delivered to the Driver [[IcebergFileCommitProtocol]] instance
 *    via [[TaskCommitMessage]].
 *  - The underlying [[FileCommitProtocol]] is setup with `dynamicPartitionOverwrite` mode
 *    set to false. Since [[IceTable]] metadata is used by scan operations to compute what
 *    files to scan we don't have to do an all-or-nothing replacement of files in a
 *    partition that is needed for dynamic partition mode using the [[FileCommitProtocol]].
 *  - in case of `dynamicPartitionOverwrite` mode we don't clear specified source Partitions, because
 *    we want the current files to be able execute queries against older snapshots.
 *  - once the job finishes the Catalog is updated with 'new' and 'deleted' partitions just as it is in
 *    a regular [[InsertIntoHadoopFsRelationCommand]]
 *  - then based on the 'initial set' of [[DataFile]] and the set of [[DataFile]] created by tasks
 *    of this job a new iceberg [[Snapshot]] is created.
 *  - finally cache invalidation and stats update actions happen just like in a regular
 *    [[InsertIntoHadoopFsRelationCommand]]
 *
 * @param outputPath
 * @param staticPartitions
 * @param ifPartitionNotExists
 * @param partitionColumns
 * @param bucketSpec
 * @param fileFormat
 * @param options
 * @param query
 * @param mode
 * @param catalogTable
 * @param fileIndex
 * @param outputColumnNames
 */
case class InsertIntoIcebergTable(outputPath: Path,
                                  staticPartitions: TablePartitionSpec,
                                  ifPartitionNotExists: Boolean,
                                  partitionColumns: Seq[Attribute],
                                  bucketSpec: Option[BucketSpec],
                                  fileFormat: FileFormat,
                                  options: Map[String, String],
                                  query: LogicalPlan,
                                  mode: SaveMode,
                                  catalogTable: CatalogTable,
                                  fileIndex: Option[FileIndex],
                                  outputColumnNames: Seq[String]
                                 ) extends DataWritingCommand {

  val partitionSchema = catalogTable.partitionSchema

  /**
   * Construct an iceberg [[com.netflix.iceberg.TableScan]], optionally apply a filter based on
   * the `staticPartitions` provide on this invocation. Execute the scan to get a [[DataFile]],
   * and build a set of [[TablePartitionSpec]] from the datafiles.
   *
   * @param icebergTable
   * @return
   */
  private def matchIceDataFiles(icebergTable :IceTable) :
  (Seq[DataFile], Set[TablePartitionSpec]) = {
    val spkPartExpression : Option[Expression] = IcePartitioningUtils.
      expressionFromPartitionSpec(partitionSchema,CaseInsensitiveMap(staticPartitions))
    val iceExpr = spkPartExpression.flatMap(ExpressionConversions.convertStrict(_))
    var iceScan = icebergTable.newScan()

    if (iceExpr.isDefined) {
      iceScan = iceScan.filter(iceExpr.get)
    }

    import scala.collection.JavaConversions._
    val dfs = iceScan.planFiles().map(_.file).toSeq
    val matchingPSepc = (dfs.map(df => DataFileUtils.sparkPartitionMap(df))).toSet
    (dfs, matchingPSepc)
  }

  private def checkNoCustomPartitionLocations(fs: FileSystem,
                                              qualifiedOutputPath: Path
                                             )(implicit sparkSession: SparkSession) : Unit = {

    val customLocPart =
      sparkSession.sessionState.catalog.listPartitions(catalogTable.identifier).find { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      catalogLocation != defaultLocation
    }

    if (customLocPart.isDefined) {
      throw new AnalysisException(
        s"Cannot have custom partition locations for a managed table"
      )
    }
  }

  private def createSnapShot(iceTable : IceTable,
                             delFiles : Seq[DataFile],
                             addFiles : Seq[DataFile]
                            ) : Unit = {

    val hasDelFiles = delFiles.nonEmpty
    val hasAddFiles = addFiles.nonEmpty

    import scala.collection.JavaConversions._
    var updt : PendingUpdate[Snapshot] = null
    (hasDelFiles, hasAddFiles) match {
      case (true, true) => {
        val r = iceTable.newRewrite()
        val ds = delFiles.toSet
        val as = addFiles.toSet
        updt = r.rewriteFiles(ds, as)
      }
      case (false, true) => {
        var r = iceTable.newAppend()
        for(f <- addFiles) {
          r =r .appendFile(f)
        }
        updt = r
      }
      case (true, false) => {
        var r = iceTable.newDelete()
        for(f <- delFiles) {
          r = r.deleteFile(f)
        }
        updt = r
      }
      case (false, false) => ()
    }

    if (updt != null) {
      updt.commit()
    }
  }

  private def _run(child: SparkPlan,
                   iceTable : IceTable
                  )(implicit sparkSession: SparkSession): Unit = {

    // Most formats don't do well with duplicate columns, so lets not allow that
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into $outputPath",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.partitionColumnNames.nonEmpty &&
      catalogTable.tracksPartitionsInCatalog

    val (initialMatchingDataFiles: Seq[DataFile],
    initialMatchingPartitions: Set[TablePartitionSpec]) = matchIceDataFiles(iceTable)

    checkNoCustomPartitionLocations(fs, qualifiedOutputPath)

    val pathExists = fs.exists(qualifiedOutputPath)
    val parameters = CaseInsensitiveMap(options)
    val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
      // scalastyle:off caselocale
      .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase))
      // scalastyle:on caselocale
      .getOrElse(sparkSession.sessionState.conf.partitionOverwriteMode)

    val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
    // This config only makes sense when we are overwriting a partitioned dataset with dynamic
    // partition columns.
    val dynamicPartitionOverwrite = enableDynamicOverwrite && mode == SaveMode.Overwrite &&
      staticPartitions.size < partitionColumns.length

    val committer : IcebergFileCommitProtocol = {
      val designate = FileCommitProtocol.instantiate(
        sparkSession.sessionState.conf.fileCommitProtocolClass,
        jobId = java.util.UUID.randomUUID().toString,
        outputPath = outputPath.toString,
        dynamicPartitionOverwrite = false
      )

      new IcebergFileCommitProtocol(designate,
      outputPath.toString,
      partitionSchema,
      iceTable.spec(),
      iceTable.schema(),
        utils.iceFormat(fileFormat)
      )
    }

    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        if (ifPartitionNotExists && initialMatchingDataFiles.nonEmpty) {
          false
        } else {
          true
        }
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
      case (s, exists) =>
        throw new IllegalStateException(s"unsupported save mode $s ($exists)")
    }

    if (doInsertion) {

      def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
        if (partitionsTrackedByCatalog) {
          val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
          val newPartitions = updatedPartitions -- initialMatchingPartitions
          if (newPartitions.nonEmpty) {
            AlterTableAddPartitionCommand(
              catalogTable.identifier, newPartitions.toSeq.map(p => (p, None)),
              ifNotExists = true).run(sparkSession)
          }
          // For dynamic partition overwrite, we never remove partitions but only update existing
          // ones.
          if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
            val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
            if (deletedPartitions.nonEmpty) {
              AlterTableDropPartitionCommand(
                catalogTable.identifier, deletedPartitions.toSeq,
                ifExists = true, purge = false,
                retainData = true /* already deleted */).run(sparkSession)
            }
          }
        }
      }

      val updatedPartitionPaths =
        FileFormatWriter.write(
          sparkSession = sparkSession,
          plan = child,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(
            qualifiedOutputPath.toString, Map.empty, outputColumns),
          hadoopConf = hadoopConf,
          partitionColumns = partitionColumns,
          bucketSpec = bucketSpec,
          statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
          options = options)

      createSnapShot(iceTable,
        if (mode == SaveMode.Overwrite) initialMatchingDataFiles else Seq.empty,
        committer.addedDataFiles
      )

      // update metastore partition metadata
      if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
        && partitionColumns.length == staticPartitions.size) {
        // Avoid empty static partition can't loaded to datasource table.
        val staticPathFragment =
          PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
        refreshUpdatedPartitions(Set(staticPathFragment))
      } else {
        refreshUpdatedPartitions(updatedPartitionPaths)
      }

      // refresh cached files in FileIndex
      fileIndex.foreach(_.refresh())
      // refresh data cache if table is cached
      sparkSession.catalog.refreshByPath(outputPath.toString)

      CommandUtils.updateTableStats(sparkSession, catalogTable)

    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

  }

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {

    implicit  val ss = sparkSession
    val iceTables = new SparkTables(ss, catalogTable)
    val tbId = utils.qualifiedTableIdentifier(catalogTable.identifier, ss)
    val iceTable = iceTables.load(tbId.database.getOrElse(null), tbId.table)

    _run(child, iceTable)
    Seq.empty[Row]
  }
}

object InsertIntoIcebergTable {
  def apply(ihr: InsertIntoHadoopFsRelationCommand,
            catalogTable: CatalogTable
           ) : InsertIntoIcebergTable = {
    InsertIntoIcebergTable(
      ihr.outputPath,
      ihr.staticPartitions,
      ihr.ifPartitionNotExists,
      ihr.partitionColumns,
      ihr.bucketSpec,
      ihr.fileFormat,
      ihr.options,
      ihr.query,
      ihr.mode,
      catalogTable,
      ihr.fileIndex,
      ihr.outputColumnNames
    )
  }
}