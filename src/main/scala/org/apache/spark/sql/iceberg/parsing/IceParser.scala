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

package org.apache.spark.sql.iceberg.parsing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.iceberg.utils
import org.apache.spark.sql.iceberg.utils.TableUtils
import org.apache.spark.sql.iceberg.utils.TableUtils.SNAPSHOTSVIEW_SUFFIX
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.Try

class SparkIceParser(baseParser : ParserInterface) extends ParserInterface {

  val iceParser = new IceParser(baseParser)

  override def parsePlan(sqlText: String): LogicalPlan = {

    val splParsedPlan = Try {
      iceParser.parse2(sqlText)
    }.getOrElse(iceParser.Failure("Not valid ice extension", null))

    if (splParsedPlan.successful ) {
      splParsedPlan.get
    } else {
      try {
        baseParser.parsePlan(sqlText)
      } catch {
        case pe : ParseException => {
          val splFailureDetails = splParsedPlan.asInstanceOf[IceParser#NoSuccess].msg
          throw new ParseException(pe.command,
            pe.message + s"\nIce parse attempt message: $splFailureDetails",
            pe.start,
            pe.stop
          )
        }
      }
    }
  }

  def parseExpression(sqlText: String): Expression =
    baseParser.parseExpression(sqlText)

  def parseTableIdentifier(sqlText: String): TableIdentifier =
    baseParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    baseParser.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    baseParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    baseParser.parseDataType(sqlText)
}


class IceParser(val baseParser : ParserInterface) extends AbstractSparkSQLParser {

  def sparkSession = SparkSession.getActiveSession.get

  def parse2(input: String): ParseResult[LogicalPlan] = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input))
  }

  protected override lazy val start: Parser[LogicalPlan] =
    selectSnapshotViewStar | selectSnapshotViewProject | asOfSelect

  private lazy val selectSnapshotViewStar : Parser[LogicalPlan] =
    (SELECT ~ STAR ~ FROM) ~> qualifiedId ^^ {
      case tblNm if tblNm.endsWith(TableUtils.SNAPSHOTSVIEW_SUFFIX) =>
        TableUtils.snapShotsLocalRelation(
          tblNm.substring(0, tblNm.length - SNAPSHOTSVIEW_SUFFIX.length)
        )(sparkSession)
  }

  private lazy val selectSnapshotViewProject : Parser[LogicalPlan] =
    (SELECT ~> qualifiedCols) ~ (FROM ~> qualifiedId) ^^ {
      case qCols ~ tblNm if tblNm.endsWith(TableUtils.SNAPSHOTSVIEW_SUFFIX) =>
        val sRel = TableUtils.snapShotsLocalRelation(
          tblNm.substring(0, tblNm.length - SNAPSHOTSVIEW_SUFFIX.length)
        )(sparkSession)
        Project(qCols.map(UnresolvedAttribute(_)), sRel)
    }

  private lazy val asOfSelect : Parser[LogicalPlan] =
    (AS ~ OF) ~> stringLit  ~ restInput ^^ {
      case asOfTime ~ query => {
        TableUtils.setThreadSnapShotMillis(utils.convertToEpoch(asOfTime))
        baseParser.parsePlan(query)
      }
    }

  private lazy val qualifiedCols : Parser[Seq[String]] =
    repsep(qualifiedCol, ",")

  private lazy val qualifiedCol : Parser[String] =
    (ident ~ opt("." ~> ident) ~ opt("." ~> ident)) ^^ {
      case c ~ None ~ None => c
      case t ~ Some(c) ~ None => s"$t.$c"
      case d ~ Some(t) ~ Some(c) => s"$d.$t.$c"
    }

  private lazy val qualifiedId : Parser[String] =
    (ident ~ ("." ~> ident).?) ^^ {
      case ~(n, None) => n
      case ~(q, Some(n)) => s"$q.$n"
    }

  protected val SELECT = Keyword("SELECT")
  protected val STAR = Keyword("*")
  protected val FROM = Keyword("FROM")
  protected val AS = Keyword("AS")
  protected val OF = Keyword("OF")

}
