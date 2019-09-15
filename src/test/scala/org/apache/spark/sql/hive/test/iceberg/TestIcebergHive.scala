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

import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.internal.SQLConf

object TestIcebergHive extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set(SQLConf.CODEGEN_FALLBACK.key, "false")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        .set("spark.sql.warehouse.dir", TestHiveContext.makeWarehouseDir().toURI.getPath)
        // SPARK-8910
        .set("spark.ui.enabled", "false")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
        // Disable ConvertToLocalRelation for better test coverage. Test cases built on
        // LocalRelation will exercise the optimization rules better by disabling it as
        // this rule may potentially block testing of other optimization rules such as
        // ConstantPropagation etc.
        .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
        .set("spark.sql.extensions", "org.apache.spark.sql.iceberg.planning.SparkSessionExtensions")
    ), false
)