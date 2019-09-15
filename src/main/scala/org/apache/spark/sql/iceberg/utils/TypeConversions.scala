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

import com.netflix.iceberg.types.{Type, Types}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object TypeConversions {

  def convertStructType(st : StructType) : TypeMapping = {
    implicit val nextId = new Function0[Int] {
      var cnt = -1
      def apply : Int = {
        cnt += 1
        cnt
      }
    }
    TypeMapping(st, structType(st).asInstanceOf[Types.StructType])
  }

  def toIcebergType(dt : DataType)(implicit nextId : () => Int) : Type =  dt match {
    case st : StructType => structType(st)
    case mt : MapType => mapType(mt)
    case at : ArrayType => arrType(at)
    case at : AtomicType => atomicType(at)
    case _ =>
      throw new UnsupportedOperationException(s"Spark datatype ${dt.toString} is not supported")
  }

  def structType(st : StructType)(implicit nextId : () => Int) : Type = {
    import scala.collection.JavaConversions._

    val iceFields : java.util.List[Types.NestedField] = (
      for( (field, i) <- st.fields.zipWithIndex) yield {

      val (id, nm, typ) = (nextId(), field.name, toIcebergType(field.dataType))
      if (field.nullable) {
        Types.NestedField.optional(id, nm, typ)
      } else Types.NestedField.required(id, nm, typ)

    }).toList

    Types.StructType.of(iceFields)
  }

  def mapType(mT : MapType)(implicit nextId : () => Int) : Type = {

    val (kId,vId,kT, vT) =
      (nextId(), nextId(), toIcebergType(mT.keyType), toIcebergType(mT.valueType))

    if (mT.valueContainsNull) {
      Types.MapType.ofOptional(kId, vId, kT, vT)
    }
    else Types.MapType.ofRequired(kId,vId,kT, vT)
  }

  def arrType(aT : ArrayType)(implicit nextId : () => Int) : Type = {

    val (eId,eT) = (nextId(), toIcebergType(aT.elementType))

    if (aT.containsNull) {
      Types.ListType.ofOptional(eId, eT)
    }
    else Types.ListType.ofRequired(eId, eT)
  }


  def atomicType(aT: AtomicType): Type = aT match {
    case b : BooleanType => Types.BooleanType.get
    case i : IntegerType => Types.IntegerType.get
    case s : ShortType => Types.IntegerType.get
    case b : ByteType => Types.IntegerType.get
    case l : LongType => Types.LongType.get
    case f : FloatType => Types.FloatType.get
    case d : DoubleType => Types.DoubleType.get
    case s : StringType => Types.StringType.get
    case c : CharType => Types.StringType.get
    case c : VarcharType => Types.StringType.get
    case d : DateType => Types.DateType.get
    case t : TimestampType => Types.TimestampType.withZone
    case d : DecimalType => Types.DecimalType.of(d.precision, d.scale)
    case b : BinaryType => Types.BinaryType.get
    case _ => throw new UnsupportedOperationException("Not a supported type: " + aT.catalogString)
  }

  case class TypeMapping(sparkType : StructType,
                         iceType : Types.StructType) {

    def findField(nm : String)(implicit sparkSession : SparkSession) :
    Option[(StructField, Types.NestedField)] = {
      (sparkType.fields.find {
        case f if sparkSession.sqlContext.conf.resolver(f.name, nm) => true
        case _ => false
      }).map(t => (t, iceType.field(t.name) ))
    }
  }

}
