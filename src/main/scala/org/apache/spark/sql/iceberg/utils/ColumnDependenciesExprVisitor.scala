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

import com.netflix.iceberg.expressions.{BoundPredicate, Expression, Expressions, UnboundPredicate}
import com.netflix.iceberg.expressions.ExpressionVisitors.ExpressionVisitor
import com.netflix.iceberg.transforms.{Transform => IceTransform}
import com.netflix.iceberg.types.Types
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class ColumnDependenciesExprVisitor(iceType : Types.StructType,
                                    columnDep : Map[String, Map[String, IceTransform[_,_]]]
                                   ) extends ExpressionVisitor[Expression] {

  val caseInsensitiveMap : Map[String, Map[String, IceTransform[_,_]]]
  = CaseInsensitiveMap(columnDep)

  override def alwaysTrue(): Expression = Expressions.alwaysTrue

  override def alwaysFalse(): Expression = Expressions.alwaysFalse()

  override def not(result: Expression): Expression = Expressions.not(result)

  override def and(leftResult: Expression, rightResult: Expression): Expression =
    Expressions.and(leftResult, rightResult)

  override def or(leftResult: Expression, rightResult: Expression): Expression =
    Expressions.or(leftResult, rightResult)

  override def predicate[T](pred: BoundPredicate[T]): Expression = pred

  override def predicate[T](pred: UnboundPredicate[T]): Expression = {

    val boundExpr = pred.bind(iceType.asStructType(), false)
    val srcCol = pred.ref().name

    if (boundExpr.isInstanceOf[BoundPredicate[_]]) {
      val bndPred = boundExpr.asInstanceOf[BoundPredicate[T]]
      val transformPreds : Seq[Expression] = (for (
        transformsMap <- caseInsensitiveMap.get(srcCol).toSeq;
        (destCol, iceTrans) <- transformsMap.iterator
      ) yield {
          iceTrans.asInstanceOf[IceTransform[T,_]].project(destCol, bndPred)
      }).filter(_ != null)

      transformPreds.foldLeft(pred.asInstanceOf[Expression])(Expressions.and(_,_))

    } else {
      pred
    }
  }

}
