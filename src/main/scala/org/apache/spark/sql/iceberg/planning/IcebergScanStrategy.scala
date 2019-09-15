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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FileSourceStrategy, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.iceberg.Config.isManaged
import org.apache.spark.sql.iceberg.IceTableScanExec

case class IcebergScanStrategy(ss: SparkSession) extends Strategy with Logging {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
    l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table, _))
    if table.isDefined && isManaged(table.get) => {
      FileSourceStrategy(plan).map { plan =>
        plan.transformUp {
          case fsScan: FileSourceScanExec => {
            IceTableScanExec(fsScan, table.get)
          }
        }
      }
    }
    case _ => Nil
  }

}
