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

import com.netflix.iceberg.transforms.{Transform => IceTransform, Transforms => IcebergTransforms}
import com.netflix.iceberg.types.{Type, Types}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.iceberg.Config
import org.apache.spark.sql.iceberg.utils.TypeConversions.TypeMapping
import org.apache.spark.sql.types.StructField

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.util.Try

object Transforms {

  val TRANSFORM_REGX = """(([^\s,=:]+=[^\s,=:]+:[^\s,=:]+)(?:,\s*)?)+""".r

  /**
    * Validate [[Config.COLUMN_DEPENDENCIES]] specified for a ''Managed Table''.
    * - the option must be in the form of a comma-separated list of ''srCol=destCol:tnExpr''
    * - each ''srCol'' must resolve to a attribute in the given [[TypeMapping.sparkType]]
    * - each ''destCol'' must resolve to a attribute in the given [[TypeMapping.sparkType]]
    * - each ''tnExpr'' must resolve to a valid [[Transforms]] for the column's [[Type]]
    *
    * @param valueMappings
    * @param typeMapping
    * @param sparkSession
    * @return
    */
  def fromOption(valueMappings : String,
                 typeMapping : TypeMapping
                )(implicit sparkSession : SparkSession) :
  Either[Array[String],Map[String, Map[String, IceTransform[_,_]]]] = {

    val errs = ArrayBuffer[String]()
    val transforms = MMap[String, Map[String, IceTransform[_,_]]]()

    def parseTransforms : Array[String] = {
      valueMappings.split(",").map(_.trim)
    }

    def parseEntry(s : String) : Option[(String,String,String)] = {
      val kv = s.split("=")
      if (kv.size != 2) {
        errs +=
          s"Cannot parse transform ${s} in partition transformations: ${valueMappings}"
        None
      } else {
        val vSplit = kv(1).split(":")
        if (vSplit.size != 2) {
          errs +=
            s"Cannot parse transform ${s} in partition transformations: ${valueMappings}"
          None
        } else {
          Some((kv(0).trim, vSplit(0).trim, vSplit(1).trim))
        }
      }
    }

    def validateAttr(attr : String) : Option[(StructField, Types.NestedField)] = {
      typeMapping.findField(attr)
    }

    def validateTransform(iceF : Types.NestedField,
                          t: String) : Option[IceTransform[_,_]] = {

      Try {
        Some(IcebergTransforms.fromString(iceF.`type`(), t))
      }.recover {
        case throwable: Throwable =>
          errs += s"Failed to parse transform ${t} in partition transformations: " +
            s"${valueMappings}; (${throwable.getMessage})"
          None
      }.get
    }

    for(
      transform <- parseTransforms;
      (sc, pc,v) <- parseEntry(transform) ;
      (_, sIceF) <- validateAttr(sc);
      t <- validateTransform(sIceF, v);
      (_, pIceF) <- validateAttr(pc)
    ) {
      val tm = transforms.getOrElse(sIceF.name, Map[String, IceTransform[_,_]]())
      transforms(sIceF.name) = (tm + ((pIceF.name(), t)))
    }

    if (errs.nonEmpty) {
      Left(errs.toArray)
    } else {
      Right(transforms.toMap)
    }

  }
}
