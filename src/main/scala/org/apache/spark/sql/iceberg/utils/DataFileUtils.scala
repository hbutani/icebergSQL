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

import com.netflix.iceberg.hadoop.HadoopInputFile
import com.netflix.iceberg.{DataFile, DataFiles, Metrics, PartitionSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.iceberg.PartitioningUtils
import org.apache.spark.sql.types.StructType

/**
 * Utilities to construct and extract information from [[DataFile]]
 */
object DataFileUtils {

  def buildDataFileFormPath(pathString : String,
                            iceSchema: IceSchema,
                            partSchema: StructType,
                            partSpec : PartitionSpec,
                            iceFmt : IceFormat,
                            conf : Configuration,
                            finalPath : String
                           ) : DataFile = {

    val path = new Path(pathString)
    var bldr = DataFiles.builder(partSpec)
    val inFile = HadoopInputFile.fromPath(path, conf)
    bldr.withInputFile(inFile)

    val metrics : Option[Metrics] = iceMetrics(inFile, iceSchema, iceFmt)
    val pSpec = icePartStruct(pathString, partSchema)

    bldr.withPartition(pSpec)
    bldr.withFormat(iceFmt)
    if (metrics.isDefined) {
      bldr.withMetrics(metrics.get)
    }
    bldr.withPath(finalPath)
    bldr.build()
  }

  def sparkPartitionMap(df : DataFile) : Map[String, String] =
    PartitioningUtils.partitionValuesFromFullPath(
      PartitioningUtils.removeFileName(df.path().toString)
    )


}
