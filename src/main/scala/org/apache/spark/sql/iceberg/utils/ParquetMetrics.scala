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

import java.io.IOException
import java.lang.{Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.util

import com.netflix.iceberg.Metrics
import com.netflix.iceberg.exceptions.RuntimeIOException
import com.netflix.iceberg.expressions.Literal
import com.netflix.iceberg.hadoop.HadoopInputFile
import com.netflix.iceberg.types.Conversions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import scala.collection.mutable.{Map => MMap, Set => MSet}
import scala.language.existentials

object ParquetMetrics {

  def fromInputFile(file: HadoopInputFile,
                    iceSchema: IceSchema): Metrics = {
    val parqFile =
      org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(file.getStat, file.getConf)
    val reader = ParquetFileReader.open(parqFile)
    try {
      fromMetadata(reader.getFooter, iceSchema)
    } catch {
      case e: IOException =>
        throw new RuntimeIOException(e, "Failed to read footer of file: %s", file)
    } finally if (reader != null) reader.close()
  }

  def fromMetadata(metadata: ParquetMetadata,
                   fileSchema: IceSchema
                  ): Metrics = {

    var rowCount: Long = 0L
    val columnSizes = MMap[JInt, JLong]()
    val valueCounts = MMap[JInt, JLong]()
    val nullValueCounts = MMap[JInt, JLong]()
    val lowerBounds = MMap[JInt, IceLiteral[_]]()
    val upperBounds = MMap[JInt, IceLiteral[_]]()

    val missingStats = MSet[JInt]()

    val parquetType = metadata.getFileMetaData.getSchema
    val blocks = metadata.getBlocks
    import scala.collection.JavaConversions._
    for (block <- blocks) {
      rowCount += block.getRowCount
      import scala.collection.JavaConversions._
      for (column <- block.getColumns) {

        val field = fileSchema.findField(column.getPath.toDotString)

        if (field != null) {
          val fieldId = field.fieldId()
          increment(columnSizes, fieldId, column.getTotalSize)
          increment(valueCounts, fieldId, column.getValueCount)
          val stats = column.getStatistics

          if (stats == null) {
            missingStats.add(fieldId)
          }
          else if (!stats.isEmpty) {
            increment(nullValueCounts, fieldId, stats.getNumNulls)
            val field = fileSchema.asStruct.field(fieldId)
            if (field != null && stats.hasNonNullValue) {
              updateMin(
                lowerBounds,
                fieldId,
                LiteralUtils.fromParquetPrimitive(stats.genericGetMin, field.`type`)
              )
              updateMax(
                upperBounds,
                fieldId,
                LiteralUtils.fromParquetPrimitive(stats.genericGetMax, field.`type`)
              )
            }
          }
        }
      }
    }
    // discard accumulated values if any stats were missing
    for (fieldId <- missingStats) {
      nullValueCounts.remove(fieldId)
      lowerBounds.remove(fieldId)
      upperBounds.remove(fieldId)
    }

    new Metrics(
      rowCount,
      javaMap(columnSizes.toMap),
      javaMap(valueCounts.toMap),
      javaMap(nullValueCounts.toMap),
      javaMap(toBufferMap(fileSchema, lowerBounds)),
      javaMap(toBufferMap(fileSchema, upperBounds))
    )
  }

  private def increment(columns: MMap[JInt, JLong],
                        fieldId: Int,
                        amount: JLong): Unit = {
    val v: JLong = columns.getOrElse(fieldId, 0L)
    columns(fieldId) = (v + amount)
  }

  private def updateMin[T](lowerBounds: MMap[JInt, Literal[_]],
                           id: Int,
                           min: Literal[T]
                          ) = {
    val currentMinO: Option[Literal[_]] = lowerBounds.get(id)

    def currentMin: Literal[T] = currentMinO.get.asInstanceOf[Literal[T]]

    if (!currentMinO.isDefined ||
      min.comparator().compare(min.value(), currentMin.value()) < 0
    ) {
      lowerBounds(id) = min
    }
  }

  private def updateMax[T](upperBounds: MMap[JInt, Literal[_]],
                           id: Int,
                           max: Literal[T]
                          ) = {
    val currentMaxO: Option[Literal[_]] = upperBounds.get(id)

    def currentMax: Literal[T] = currentMaxO.get.asInstanceOf[Literal[T]]

    if (!currentMaxO.isDefined ||
      max.comparator().compare(max.value(), currentMax.value()) > 0
    ) {
      upperBounds(id) = max
    }
  }

  private def toBufferMap(schema: IceSchema,
                          map: MMap[JInt, IceLiteral[_]]
                         ): Map[JInt, ByteBuffer] = {

    val mb = MMap[JInt, ByteBuffer]()

    for ((i, l) <- map.iterator) {
      val t = schema.findType(i)
      mb(i) = Conversions.toByteBuffer(t, l.value())
    }

    mb.toMap
  }

  private def javaMap[K,V](m : Map[K,V]) : java.util.Map[K,V] = {
    val jm = new util.HashMap[K,V](m.size)
    for(
      (k,v) <- m
    ) {
      jm.put(k,v)
    }
    jm
  }

}
