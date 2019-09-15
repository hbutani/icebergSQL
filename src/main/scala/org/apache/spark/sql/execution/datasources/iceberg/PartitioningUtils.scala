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

package org.apache.spark.sql.execution.datasources.iceberg

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Cast, EqualTo, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{PartitionPath, PartitionSpec => SparkPSec, PartitioningUtils => SparkPartitioningUtils}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * '''Partitioning Data Structures and concepts'''
 *
 * 1. ''Partitioning Scheme:'' represents the structure of a Table's partitions. Represented
 * as a Spark [[StructType]]
 * 2. ''Partition Value or Row:'' represents the values of a Table partition. Represented
 * as a Spark [[InternalRow]]
 * 3. ''Partition Specification:'' represents a set of partitions being acted on in a ''Insert''
 * commad. Capture as a [[TablePartitionSpec]]
 * 4. ''Partition Info : '' details about a Table Partition captured as [[PartitionPath]] (this
 * name is confusing)
 * 5. ''Partition Set :'' represents details on a set of Table Partitions captured as
 * [[PartitionSpec]](terrible name)
 */
object PartitioningUtils {

  /**
   * A wrapper for the [[SparkPartitioningUtils]] parsePartition function.
   * Used to introspect a set of filesystem paths and construct the set of
   * [[PartitionPath]] at these paths, and combine them into a [[PartitionSpec]]
   *
   * @param paths
   * @param basePaths
   * @param partSchema
   * @param sparkSession
   * @return
   */
  def parsePartitions(paths: Seq[Path],
                      basePaths: Set[Path],
                      partSchema: StructType
                     )(implicit sparkSession: SparkSession) : SparkPSec = {

    val tzId = sparkSession.sessionState.conf.sessionLocalTimeZone

    SparkPartitioningUtils.parsePartitions(
      paths,
      typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
      basePaths = basePaths,
      Some(partSchema),
      sparkSession.sqlContext.conf.caseSensitiveAnalysis,
      DateTimeUtils.getTimeZone(tzId))
  }

  /**
   * A thin wrapper on `parsePartitions` method. This converts a filesystem path
   * into a ''Partition Info'', returning it as a [[PartitionPath]]
   * @param p
   * @param partSchema
   * @param sparkSession
   * @return
   */
  def parsePath(p : Path,
                partSchema: StructType
               )(implicit sparkSession: SparkSession) : PartitionPath = {
    val sPSpec = parsePartitions(Seq(p), Set.empty, partSchema)
    sPSpec.partitions.head
  }

  /**
   * Convert a fileSystem path into a ''Partition Value'' return it as an [[InternalRow]]
   * @param p
   * @param partSchema
   * @return
   */
  def partitionRowFromFullPath(p : String, partSchema : StructType) : InternalRow = {
    val pathString = removeFileName(p)
    val m = CaseInsensitiveMap(partitionValuesFromFullPath(pathString))

    assert(m.size == partSchema.size)

    val casts : Array[Cast] = for(
      f <- partSchema.fields
    ) yield {
      val s = m(f.name)
      val l = Literal.create(UTF8String.fromString(s), StringType)
      Cast(l, f.dataType)
    }
    val values : Seq[Any] = casts.map(c => c.eval(null))
    new GenericInternalRow(values.toArray)

  }

  /**
   * Convert a ''Partition Value'' into a [[Expression][Spark Expression]].
   * The Expression is a conjunct of Equalty conditions. For each partition
   * column we have an equality condition that the partition column equals the value in
   * the given partition Row/value.
   *
   * @param partSchema
   * @param partRow
   * @return
   */
  def partitionRowToPredicate(partSchema : StructType,
                              partRow : InternalRow) : Expression = {

    val exprs = for((f,i) <- partSchema.zipWithIndex) yield {
      val attr = AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      EqualTo(attr, Literal(partRow.get(i, f.dataType)))
    }
    exprs.reduce(And)
  }

  /**
   * Parse a filesystem path into a ''Partition Specification:'' and return it as
   * [[TablePartitionSpec]]. Look for folder names of the form 'name=value' at the end of the
   * path and map them into entries in the [[TablePartitionSpec]]
   *
   * @param pathString
   * @return
   */
  def partitionValuesFromFullPath(pathString : String) : TablePartitionSpec = {
    var m = Map.empty[String, String]
    def add(col : String, value: String) : Unit =
      m += (col -> value)

    mapPartitionSpecs(pathString,
      (path, col, value) => add(col, value)
    )
    m
  }

  /**
   * Map a [[TablePartitionSpec]] into a [Expression][Spark Expression]].
   * The Expression is a conjunct of Equalty conditions. For each partition
   * column we have an equality condition that the partition column equals the value in
   * the given [[TablePartitionSpec]]
   *
   * @param partitionSchema
   * @param pSpec
   * @return
   */
  def expressionFromPartitionSpec(partitionSchema : StructType,
                                  pSpec : CaseInsensitiveMap[String]
                                 ) : Option[Expression] = {
    val exprs = for(
      attr <- partitionSchema.toAttributes;
      v <- pSpec.get(attr.name)
    ) yield {
      EqualTo(attr, Cast(Literal(UTF8String.fromString(v), StringType), attr.dataType))
    }

    exprs match {
      case e if e.size == 0 => None
      case e if e.size == 1 => Some(e.head)
      case _ => Some(exprs.reduce(And))
    }
  }

  private def mapPartitionSpecs(fullPath : String,
                                op : (String, String, String) => Unit) : Unit = {
    var p = new Path(fullPath)
    var reachedNonPartName = false

    while(!reachedNonPartName) {
      val name = p.getName
      partitionSpec(name) match {
        case Some((col, value)) => op(name, col,value)
        case None => {
          reachedNonPartName = true
        }
      }
      p = p.getParent
    }
  }

  private def partitionSpec(fileName : String) : Option[(String,String)] = {
    val equalSignIndex = fileName.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = fileName.take(equalSignIndex)
      if (columnName.isEmpty) return None
      val rawColumnValue = fileName.drop(equalSignIndex + 1)
      if (rawColumnValue.nonEmpty) {
        Some((columnName, rawColumnValue))
      } else None
    }
  }

  def removeFileName(pathStr : String): String = {
    if (pathStr.endsWith("/")) {
      pathStr
    } else {
      val path = new Path(pathStr)
      path.getParent.toString
    }
  }

}
