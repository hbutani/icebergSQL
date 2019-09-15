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

package org.apache.spark.sql.iceberg.planning

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand}
import org.apache.spark.sql.iceberg.Config.provideManagement
import org.apache.spark.sql.iceberg.{CreateIcebergTable, CreateIcebergTableAsSelect}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object CreateTableCheck extends Rule[LogicalPlan] {

  import IcebergTableValidationChecks.validate

  private def checkPlan(plan : LogicalPlan) : Unit = plan match {
    case CreateDataSourceTableCommand(table, ignoreIfExists) if provideManagement(table) => {
      val errs = ArrayBuffer[String]()
      if (ignoreIfExists) {
        errs + "managed table supported only when 'ignoreIfExists' option is false"
      }
      validate("create", table, errs)

    }
    case CreateDataSourceTableAsSelectCommand(table, mode, _, _) if provideManagement(table) => {
      val errs = ArrayBuffer[String]()
      if (mode != SaveMode.ErrorIfExists) {
        errs + "managed table supported only for SaveMode = 'ErrorIfExists'"
      }
      validate("create as", table, errs)
    }
    case _ => ()
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    checkPlan(plan)
    plan
  }

}

case class CreateIcebergTableRule(ss: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case ct@CreateDataSourceTableCommand(table, ignoreIfExists) if provideManagement(table) => {
      CreateIcebergTable(ct)
    }
    case ct@CreateDataSourceTableAsSelectCommand(table, mode, _, _) if provideManagement(table) => {
      CreateIcebergTableAsSelect(ct)
    }
  }
}
