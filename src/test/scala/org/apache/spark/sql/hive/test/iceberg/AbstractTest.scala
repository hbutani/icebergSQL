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

package org.apache.spark.sql.hive.test.iceberg

import java.io.File
import java.util.TimeZone

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.iceberg.planning.SparkSessionExtensions
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTimeZone
import org.scalatest.{BeforeAndAfterAll, fixture}
import org.apache.spark.sql.iceberg.utils

abstract class AbstractTest extends fixture.FunSuite
  with  fixture.TestDataFixture with BeforeAndAfterAll with TestTables with Logging {

  override def beforeAll() = {
    println("*** Starting TestCase " ++ this.toString() )
    System.setProperty("user.timezone", "UTC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    DateTimeZone.setDefault(DateTimeZone.forID("UTC"))

    new SparkSessionExtensions()(TestIcebergHive.sparkSession.extensions)

    TestIcebergHive.sparkContext.setLogLevel("ERROR")
    TestIcebergHive.setConf("spark.sql.files.openCostInBytes", (128 * 1024 * 1024).toString)
    TestIcebergHive.setConf("spark.sql.files.maxPartitionBytes", (16 * 1024 * 1024).toString)

    setupStoreSalesTables

  }

  def result(df: DataFrame): Array[Row] = {
    df.collect()
  }

  def test(nm: String, sql: String,
           showPlan: Boolean = false,
           showResults: Boolean = false,
           setupSQL: Option[(String, String)] = None): Unit = {
    test(nm) { td =>
      println("*** *** Running Test " ++ nm)

      try {

        for((s,e) <- setupSQL) {
          TestIcebergHive.sql(s)
        }

        try {
          val df = sqlAndLog(nm, sql)
          if (showPlan) {
            logPlan(nm, df)
          }
          if (showResults) {
            df.show(20, false)
          }
        } finally {
        }
      } finally {
        for((s,e) <- setupSQL) {
          TestIcebergHive.sql(e)
        }
      }
    }
  }

  def cTest(nm: String,
            dsql: String,
            bsql: String,
            devAllowedInAproxNumeric: Double = 0.0): Unit = {
    test(nm) { td =>
      println("*** *** Running Correctness Test " ++ nm)

      try {
        val df1 = sqlAndLog(nm, dsql)
        val df2 = sqlAndLog(nm, bsql)
        assert(isTwoDataFrameEqual(df1, df2, devAllowedInAproxNumeric))
      } finally {
      }
    }
  }

  def sqlAndLog(nm: String, sqlStr: String): DataFrame = {
    logDebug(s"\n$nm SQL:\n" + sqlStr)
    TestIcebergHive.sql(sqlStr)
  }

  def logPlan(nm: String, df: DataFrame): Unit = {
    logInfo(s"\n$nm Plan:")
    logInfo(s"\nLogical Plan:\n" + df.queryExecution.logical.toString)
    logInfo(s"\nOptimized Plan:\n" + df.queryExecution.optimizedPlan.toString)
    logInfo(s"\nPhysical Plan:\n" + df.queryExecution.sparkPlan.toString)
  }

  def roundValue(chooseRounding: Boolean, v: Any, dt: DataType): Any = {
    if (chooseRounding && v != null &&
      utils.isNumeric(dt) &&
      !Set[Any](Double.PositiveInfinity, Double.NegativeInfinity,
        Float.PositiveInfinity, Float.NegativeInfinity).contains(v)
    ) {
      BigDecimal(v.toString).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else if (v == Float.PositiveInfinity){
      Double.PositiveInfinity
    } else if (v == Float.NegativeInfinity){
      Double.NegativeInfinity
    } else {
      v
    }
  }

  def isTwoDataFrameEqual(df1: DataFrame,
                          df2: DataFrame,
                          devAllowedInAproxNumeric: Double,
                          Sorted: Boolean = false,
                          chooseRounding: Boolean = true): Boolean = {


    if (df1.schema != df2.schema) {
      logWarning(
        s"""
           |different schemas issue:
           | df1 schema = ${df1.schema}
           | df2.schema = ${df2.schema}
         """.stripMargin)
      // return false
    }

    import collection.JavaConversions._

    var df11 = df1
    var df21 = df2

    var df1_ilist = df11.queryExecution.executedPlan.executeCollect()
    var df2_ilist = df21.queryExecution.executedPlan.executeCollect()


    if (!Sorted && df1_ilist.size > 1) {

      val sortCols = df11.columns

      df1_ilist = {
        df11 = utils.dataFrame(
          LocalRelation(df11.queryExecution.optimizedPlan.output, df1_ilist)
        )(TestIcebergHive.sparkSession.sqlContext)
        df11 = df11.sort(sortCols.head, sortCols.tail:_*).
          select(sortCols.head, sortCols.tail:_*)
        df11.queryExecution.executedPlan.executeCollect()
      }

      df2_ilist = {
        df21 = utils.dataFrame(
          LocalRelation(df21.queryExecution.optimizedPlan.output, df2_ilist)
        )(TestIcebergHive.sparkSession.sqlContext)
        df21 = df21.sort(sortCols.head, sortCols.tail:_*).
          select(sortCols.head, sortCols.tail:_*)
        df21.queryExecution.executedPlan.executeCollect()
      }
    }

    val df1_count = df1_ilist.size
    val df2_count = df2_ilist.size
    if (df1_count != df2_count) {
      println(df1_count + "\t" + df2_count)
      println("The row count is not equal")
      println(s"""df1=\n${df1_ilist.mkString("\n")}\ndf2=\n ${df2_ilist.mkString("\n")}""")
      return false
    }

    for (i <- 0 to df1_count.toInt - 1) {
      for (j <- 0 to df1.columns.size - 1) {
        val res1 = roundValue(chooseRounding,
          df1_ilist(i).get(j, df11.schema(j).dataType),
          df11.schema(j).dataType)
        val res2 = roundValue(chooseRounding,
          df2_ilist(i).get(j, df21.schema(j).dataType),
          df21.schema(j).dataType)
        // account for difference in null aggregation of javascript
        if (res2 == null && res1 != null) {
          if (!Set(Int.MaxValue, Int.MinValue,
            Long.MaxValue, Long.MinValue,
            Double.PositiveInfinity, Double.NegativeInfinity,
            Float.PositiveInfinity, Float.NegativeInfinity,
            0).contains(res1)) {
            println(s"values in row $i, column $j don't match: ${res1} != ${res2}")
            println(s"""df1=\n${df1_ilist.mkString("\n")}\ndf2=\n ${df2_ilist.mkString("\n")}""")
            return false
          }
        } else if ((utils.isApproximateNumeric(df1.schema(j).dataType) &&
          (Math.abs(res1.asInstanceOf[Double] - res2.asInstanceOf[Double]) >
            devAllowedInAproxNumeric)) ||
          (!utils.isApproximateNumeric(df1.schema(j).dataType) && res1 != res2)) {
          println(s"values in row $i, column $j don't match: ${res1} != ${res2}")
          println(s"""df1=\n${df1_ilist.mkString("\n")}\ndf2=\n ${df2_ilist.mkString("\n")}""")
          return false
        }
      }
    }
    logDebug("The two dataframe is equal " + df1_count)
    // println(df1_list.mkString("", "\n", ""))
    return true
  }

  def delete(f : File) : Unit = {
    if (f.exists()) {
      if (f.isDirectory) {
        f.listFiles().foreach(delete(_))
        f.delete()
      } else {
        f.delete()
      }
    }
  }


}
