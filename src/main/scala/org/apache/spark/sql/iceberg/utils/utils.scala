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

import com.netflix.iceberg.hadoop.HadoopInputFile
import com.netflix.iceberg.{DataFile, FileFormat, StructLike, Table}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasources.iceberg.PartitioningUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat => SparkFileFormat}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.util.Try

trait PlanUtils {

  def dataFrame(lP: LogicalPlan)(
    implicit sqlContext: SQLContext): DataFrame = {
    Dataset.ofRows(sqlContext.sparkSession, lP)
  }

  // for testing
  def iceTableScanExec(df : DataFrame,
                       strict : Boolean = true
                      ) : Option[IceTableScanExec] = {
    val icScanO = (df.queryExecution.executedPlan find {
      case is : IceTableScanExec => true
      case _ => false
    }).map(_.asInstanceOf[IceTableScanExec])

    if (!icScanO.isDefined & strict) {
      throw new AnalysisException(s"Failed to find a IceTableScan in given dataframe")
    }
    icScanO
  }

  // for testing
  def filesScanned(df : DataFrame,
                   strict : Boolean = true
                  ) : Seq[String] =  {
    for(
      iScan <- iceTableScanExec(df,strict).toSeq;
      pdirs = iScan.showPartitionsScanned;
      pd <- pdirs;
      fs <- pd.files
    ) yield {
      fs.getPath.toString
    }
  }
}

trait ExprUtils {

  def isApproximateNumeric(dt: DataType) = dt.isInstanceOf[FractionalType]
  def isNumeric(dt: DataType) = NumericType.acceptsType(dt)

}

trait SessionUtils {

  def currentSparkSession: SparkSession = {
    var spkSessionO = SparkSession.getActiveSession
    if (!spkSessionO.isDefined) {
      spkSessionO = SparkSession.getDefaultSession
    }
    spkSessionO.getOrElse(???)
  }

  def parseIdentifier(name: String,
                      sparkSession: SparkSession): TableIdentifier = {
    sparkSession.sessionState.sqlParser.parseTableIdentifier(name)
  }

  def qualifiedTableIdentifier(tabId: TableIdentifier,
                               sparkSession: SparkSession): TableIdentifier = {
    val catalog = sparkSession.sessionState.catalog

    val db = tabId.database.getOrElse(catalog.getCurrentDatabase)
    db match {
      case SessionCatalog.DEFAULT_DATABASE => TableIdentifier(tabId.table)
      case _ => TableIdentifier(tabId.table, Some(db))
    }
  }

  def dataframe(rows: Seq[Row],
                schema: StructType
               )(implicit ss: SparkSession): DataFrame = {
    ss.createDataFrame(
      ss.sparkContext.parallelize(rows, 1),
      schema
    )
  }
}

package object utils extends PlanUtils with ExprUtils with SessionUtils {

  type IcePSpec = com.netflix.iceberg.PartitionSpec
  type IceStruct = com.netflix.iceberg.StructLike
  type IceTable = com.netflix.iceberg.Table
  type IceType = com.netflix.iceberg.types.Type
  type IceSchema = com.netflix.iceberg.Schema
  type IceFormat = com.netflix.iceberg.FileFormat
  type IceMetrics = com.netflix.iceberg.Metrics
  type IceLiteral[T] = com.netflix.iceberg.expressions.Literal[T]

  class StructLikeInternalRow(val struct: StructType) extends StructLike {
    val fields : Array[StructField] = struct.fields
    val types : Array[DataType] = fields.map(_.dataType)

    private var row : InternalRow = null

    def setRow(row: InternalRow) : StructLikeInternalRow = {
      this.row = row
      this
    }

    override def size: Int = types.length

    @SuppressWarnings(Array("unchecked"))
    override def get[T](pos: Int, javaClass: Class[T]): T = types(pos) match {
      case StringType => row.getUTF8String(pos).toString.asInstanceOf[T]
      case _ => javaClass.cast(row.get(pos, types(pos)))
    }

    override def set[T](pos: Int, value: T): Unit = ???
  }

  def toIceRow(sr : InternalRow, ss : StructType) : StructLike = {
    val r = new StructLikeInternalRow(ss)
    r.setRow(sr)
    r
  }

  def iceFormat(ffName : String) : IceFormat = {
    Try {
      com.netflix.iceberg.FileFormat.valueOf(ffName)
    }.getOrElse(com.netflix.iceberg.FileFormat.AVRO)
  }

  def partitionSpec(iceTable : Table) = iceTable.spec()

  def iceFormat(ff : SparkFileFormat) : FileFormat = {
    if (ff.isInstanceOf[ParquetFileFormat]) {
      FileFormat.PARQUET
    } else {
      FileFormat.AVRO
    }
  }

  def iceMetrics(inFile: HadoopInputFile,
                 iceSchema: IceSchema,
                 iceFmt : IceFormat
                ) : Option[IceMetrics] = iceFmt match {
    case com.netflix.iceberg.FileFormat.PARQUET =>
      Some(ParquetMetrics.fromInputFile(inFile, iceSchema))
    case _ => None
  }

  def icePartStruct(pathString : String,
                    partSchema: StructType
                   ) : IceStruct = {
    val iRow = PartitioningUtils.partitionRowFromFullPath(pathString, partSchema)
    toIceRow(iRow, partSchema)
  }

}
