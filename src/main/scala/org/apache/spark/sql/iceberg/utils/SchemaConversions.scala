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

import com.netflix.iceberg.{PartitionSpec, Schema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.iceberg.utils.TypeConversions.TypeMapping

object SchemaConversions {

  private def toIcebergSchema(typeMapping: TypeMapping) : Schema = {
    new Schema(typeMapping.iceType.fields())
  }

  private def toIcebergPartitionSpec(catalogTable: CatalogTable,
                                     typeMapping: TypeMapping,
                                     schema : Schema
                                    )(implicit sparkSession : SparkSession) : PartitionSpec = {

    if (catalogTable.partitionColumnNames.nonEmpty) {
      var bldr = PartitionSpec.builderFor(schema)
      for(
        pField <- catalogTable.partitionSchema.fields;
        iceF = schema.findField(pField.name)
      ) {
        bldr = bldr.identity(iceF.name())
      }
      bldr.build()
    } else {
      null
    }
  }

  def toIcebergSpec(catalogTable: CatalogTable)(implicit sparkSession : SparkSession) :
  (Schema, PartitionSpec) = {

    val typMap = TypeConversions.convertStructType(catalogTable.schema)
    val schema = toIcebergSchema(typMap)
    val pSpec = toIcebergPartitionSpec(catalogTable, typMap, schema)

    (schema, pSpec)

  }

}
