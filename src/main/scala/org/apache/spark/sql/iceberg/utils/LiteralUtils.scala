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

import java.lang.{Double => JavaDouble, Float => JavaFloat, Long => JavaLong}
import java.math.{BigDecimal => JavaBigDecimal, BigInteger => JavaBigInteger}

import com.netflix.iceberg.expressions.Literal.{of => ofIceLiteral}
import com.netflix.iceberg.types.Types
import org.apache.commons.io.Charsets
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types._
import org.apache.parquet.io.api.Binary

/**
 * Utilities to convert from and to [[IceLiteral]]
 */
object LiteralUtils {

  object IcebergLiteral {
    def unapply(e: Any): Option[IceLiteral[_]] = e match {
      case l: Literal => (l.value, l.dataType) match {
        case (_, StringType) => Some(ofIceLiteral(l.value.toString))
        case (_, dt: DecimalType) =>
          Some(ofIceLiteral(l.value.asInstanceOf[Decimal].toJavaBigDecimal))
        case (v: Double, DoubleType) => Some(ofIceLiteral(v))
        case (v: Float, FloatType) => Some(ofIceLiteral(v))
        case (v: Long, LongType) => Some(ofIceLiteral(v))
        case (v: Byte, ByteType) => Some(ofIceLiteral(v))
        case (v: Short, ShortType) => Some(ofIceLiteral(v))
        case (v: Int, IntegerType) => Some(ofIceLiteral(v))
        case (v: Boolean, BooleanType) => Some(ofIceLiteral(v))
        case (v: Array[Byte], dt: ArrayType) if dt.elementType == ByteType =>
          Some(ofIceLiteral(v))
        case _ => None
      }
      case cst@Cast(lt : Literal, dt, _) => {
        val cVal = cst.eval(null)
        IcebergLiteral.unapply(Literal(cVal, dt))
      }
      case _ => None
    }
  }

  def fromParquetPrimitive[T](v: T,
                           iceType: IceType
                          ): IceLiteral[T] = (v, iceType) match {
    case (v : Boolean, _) => ofIceLiteral(v).to(iceType)
    case (v : Integer, _) => ofIceLiteral(v).to(iceType)
    case (v : JavaLong, _) => ofIceLiteral(v).to(iceType)
    case (v : JavaFloat, _) => ofIceLiteral(v).to(iceType)
    case (v : JavaDouble, _) => ofIceLiteral(v).to(iceType)
    case (v : Binary, t : Types.StringType) =>
      ofIceLiteral(Charsets.UTF_8.decode(v.toByteBuffer)).to(iceType)
    case (v : Binary, iT : Types.DecimalType) =>
      ofIceLiteral(new JavaBigDecimal(new JavaBigInteger(v.getBytes), iT.scale())).to(iceType)
    case (v : Binary, t : Types.BinaryType) => ofIceLiteral(v.toByteBuffer).to(iceType)
    case _ => throw new IllegalArgumentException("Unsupported primitive type: " + iceType)
  }

  object IcebergLiteralList {
    def unapply(eL: Seq[Any]): Option[Seq[IceLiteral[_]]] = {
      val litOps: Seq[Option[IceLiteral[_]]] = eL.map(IcebergLiteral.unapply(_))
      val zero: Option[Seq[IceLiteral[_]]] = Some(Seq.empty[IceLiteral[_]])
      litOps.foldLeft(zero) {
        case (None, _) => None
        case (Some(l), None) => None
        case (Some(l), Some(iL)) => Some(iL +: l)
      }
    }
  }
}
