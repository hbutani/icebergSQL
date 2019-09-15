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

import com.netflix.iceberg.expressions.Expression.Operation
import com.netflix.iceberg.expressions.Projections.ProjectionEvaluator
import com.netflix.iceberg.expressions.{BoundPredicate, ExpressionVisitors, Expressions, NamedReference, Predicate, UnboundPredicate, Expression => IceExpression}
import com.netflix.iceberg.transforms.{Transform => IceTransform}
import com.netflix.iceberg.types.Types
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryComparison, Expression, In, InSet, IsNotNull, IsNull, Not, Or}

object ExpressionConversions {

  import org.apache.spark.sql.iceberg.utils.LiteralUtils._

  private[iceberg] object Internals {

    val sparkToIcebergCompOp = Map[String, Operation](
      "<" -> Operation.LT,
      "<=" -> Operation.LT_EQ,
      "=" -> Operation.EQ,
      ">=" -> Operation.GT_EQ,
      ">" -> Operation.GT,
      "<=>" -> Operation.EQ
    )

    def predicate(at: Attribute, op: Operation, il: IceLiteral[_]):
    Predicate[_, NamedReference] = {
      Expressions.predicate(op, at.name, il)
    }

    object IcebergAttrLiteralCompare {
      def unapply(e: Expression): Option[IceExpression] = e match {
        case bc: BinaryComparison => (bc.left, bc.right) match {
          case (at: Attribute, IcebergLiteral(iL)) =>
            Some(predicate(at, sparkToIcebergCompOp(bc.symbol), iL))
          case (IcebergLiteral(iL), at: Attribute) =>
            Some(predicate(at, sparkToIcebergCompOp(bc.symbol).flipLR(), iL))
          case _ => None
        }
        case _ => None
      }
    }

    object IcebergIn {

      def inIceExpr(at: Attribute, vals: Seq[IceLiteral[_]]) = {
        val aF: IceExpression = Expressions.alwaysFalse()
        Some(
          vals.foldLeft(aF) {
            case (c, l) => Expressions.or(c, predicate(at, Operation.EQ, l))
          }
        )
      }

      def unapply(e: Expression): Option[IceExpression] = e match {
        case In(at: Attribute, IcebergLiteralList(vals)) => inIceExpr(at, vals)
        case InSet(at: Attribute, IcebergLiteralList(vals)) => inIceExpr(at, vals)
        case _ => None
      }
    }

    object StrictIcebergExpression {
      self =>

      def unapply(expr: Expression): Option[IceExpression] = expr match {
        case IsNull(a: Attribute) => Some(Expressions.isNull(a.name))
        case IsNotNull(a: Attribute) => Some(Expressions.notNull(a.name))
        case IcebergAttrLiteralCompare(iceExpr) => Some(iceExpr)
        case IcebergIn(iceExpr) => Some(iceExpr)
        case Not(self(iceExpr)) => Some(Expressions.not(iceExpr))
        case And(self(lIceExpr), l@self(rICeExpr)) => Some(Expressions.and(lIceExpr, rICeExpr))
        case Or(self(lIceExpr), l@self(rICeExpr)) => Some(Expressions.or(lIceExpr, rICeExpr))
        case _ => None
      }

    }

    private object CopyIceRewriteNotTransform
      extends ExpressionVisitors.ExpressionVisitor[IceExpression] {
      override def alwaysTrue(): IceExpression = Expressions.alwaysTrue()

      override def alwaysFalse(): IceExpression = Expressions.alwaysFalse()

      override def not(result: IceExpression): IceExpression = result.negate()

      override def and(leftResult: IceExpression, rightResult: IceExpression): IceExpression =
        Expressions.and(leftResult, rightResult)

      override def or(leftResult: IceExpression, rightResult: IceExpression): IceExpression =
        Expressions.or(leftResult, rightResult)

      override def predicate[T](pred: BoundPredicate[T]): IceExpression = pred

      override def predicate[T](pred: UnboundPredicate[T]): IceExpression = pred
    }

    class ApplyPartitionTransforms(val iceSchema: Types.StructType,
                                   val transformMap: Map[String, Map[String, IceTransform[_, _]]],
                                   val strict: Boolean
                                  ) extends ProjectionEvaluator {
      override def project(expr: IceExpression): IceExpression =
        ExpressionVisitors.visit(ExpressionVisitors.visit(expr, CopyIceRewriteNotTransform), this)

      override def alwaysTrue(): IceExpression = Expressions.alwaysTrue()

      override def alwaysFalse(): IceExpression = Expressions.alwaysFalse()

      override def not(result: IceExpression): IceExpression = {
        throw new UnsupportedOperationException("[BUG] project called on expression with a not")
      }

      override def and(leftResult: IceExpression, rightResult: IceExpression): IceExpression = {
        Expressions.and(leftResult, rightResult)
      }

      override def or(leftResult: IceExpression, rightResult: IceExpression): IceExpression = {
        Expressions.or(leftResult, rightResult)
      }

      override def predicate[T](pred: BoundPredicate[T]): IceExpression = {
        val sNm = iceSchema.field(pred.ref().fieldId()).name()
        val transformPreds = transformMap.get(sNm).map { transforms =>
          for ((pNm, pT) <- transforms) yield {
            if (strict) {
              (pT.asInstanceOf[IceTransform[T, _]]).projectStrict(pNm, pred)
            } else {
              (pT.asInstanceOf[IceTransform[T, _]]).project(pNm, pred)
            }
          }
        }

        transformPreds.map { tPreds =>
          tPreds.foldLeft(Expressions.alwaysTrue().asInstanceOf[IceExpression]) {
            case (c, tp) => Expressions.and(c, tp)
          }
        }.getOrElse(pred)

      }

      override def predicate[T](pred: UnboundPredicate[T]): IceExpression = {
        val bPred = pred.bind(iceSchema, true)

        bPred match {
          case bp: BoundPredicate[_] => predicate(bp)
          case _ => pred
        }
      }
    }

  }

  import Internals._

  def convertStrict(e: Expression): Option[IceExpression] =
    StrictIcebergExpression.unapply(e)

  def convert(e: Expression): IceExpression = e match {
    case StrictIcebergExpression(ice) => ice
    case And(l, r) => Expressions.and(convert(l), convert(r))
    case _ => Expressions.alwaysTrue()
  }

  def pushFiltersForScan(e : Expression,
                         iceSchema: Types.StructType,
                         transformMap: Map[String, Map[String, IceTransform[_, _]]]
                        ) : IceExpression = {
    val convExpr = convert(e)
    val partTransform =
      new ApplyPartitionTransforms(iceSchema, transformMap, false)
    partTransform.project(convExpr)
  }

}
