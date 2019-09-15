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

package org.apache.spark.sql.iceberg

import com.netflix.iceberg.{DataFile, PartitionSpec}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, TaskAttemptID}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.iceberg.utils.{DataFileUtils, _}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

/**
 * Provide the following function on top of the 'normal' Commit Protocol.
 * Commit actions are simply deferred to the 'designate' except in the following:
 * - track files created for each Task in a [[TaskPaths]] instance. This
 *   tracks the temporary file location and also the location that the
 *   file will be moved to on a commit.
 * - on Task Commit build an Iceberg [[DataFile]] instance. Currently only if the
 *   file is a parquet file we will also build column level stats.
 *   - The [[TaskCommitMessage]] we send back has a payload of [[IcebergTaskCommitMessage]],
 *     which encapsulates the TaskCommitMessage build by the  'designate' and the
 *     DataFile instances.
 * - we ignore ''deleteWithJob'' invocations, as we want to keep historical files around.
 *   These will be removed via a `clear snapshot` command.
 * - on a ''commitJob'' we extract all the [[DataFile]] instances from the
 *   [[IcebergTaskCommitMessage]] messages and expose a ''addedDataFiles'' list
 *   which is used by [[IceTableScanExec]] to build the new Iceberg Table Snapshot.
 *
 * @param designate
 * @param path
 * @param partitionSchema
 * @param icePartSpec
 * @param iceSchema
 * @param iceFmt
 */
class IcebergFileCommitProtocol(val designate: FileCommitProtocol,
                                val path: String,
                                val partitionSchema: StructType,
                                val icePartSpec: PartitionSpec,
                                val iceSchema: IceSchema,
                                val iceFmt: IceFormat
                               )
  extends FileCommitProtocol with Serializable {

  import org.apache.spark.sql.iceberg.IcebergFileCommitProtocol._

  @transient var taskFiles = MMap[TaskAttemptID, ArrayBuffer[TaskPaths]]()
  @transient val addedDataFiles = ArrayBuffer[DataFile]()

  private def setupTaskFileMap: Unit = {
    if (taskFiles == null) {
      taskFiles = MMap[TaskAttemptID, ArrayBuffer[TaskPaths]]()
    }
  }

  override def setupJob(jobContext: JobContext): Unit =
    designate.setupJob(jobContext)

  override def commitJob(jobContext: JobContext,
                         taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]
                        ): Unit = {
    val desgMgs = taskCommits.map(m =>
      new FileCommitProtocol.TaskCommitMessage(
        m.obj.asInstanceOf[IcebergTaskCommitMessage].designatePayload
      )
    )
    designate.commitJob(jobContext, desgMgs)
    for (
      tasmCmtMsg <- taskCommits
    ) {
      val iceTaskMsg = tasmCmtMsg.obj.asInstanceOf[IcebergTaskCommitMessage]
      addedDataFiles.appendAll(iceTaskMsg.dataFiles)
    }
  }

  override def abortJob(jobContext: JobContext): Unit =
    designate.abortJob(jobContext)

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    designate.setupTask(taskContext)
    setupTaskFileMap
    taskFiles(taskContext.getTaskAttemptID) = ArrayBuffer.empty[TaskPaths]
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String)
  : String = {
    val taskfilePath = designate.newTaskTempFile(taskContext, dir, ext)
    val fNm = new Path(taskfilePath).getName
    val finalPath = dir.map { d =>
      new Path(new Path(path, d), fNm)
    }.getOrElse {
      new Path(path, fNm)
    }

    setupTaskFileMap
    taskFiles(taskContext.getTaskAttemptID).append(TaskPaths(taskfilePath, finalPath))
    taskfilePath
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext,
                                      absoluteDir: String, ext: String
                                     ): String = {
    val taskfilePath = designate.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)
    val fNm = new Path(taskfilePath).getName
    val finalPath = new Path(absoluteDir, fNm)
    taskFiles(taskContext.getTaskAttemptID).append(TaskPaths(taskfilePath, finalPath))
    taskfilePath
  }

  override def commitTask(taskContext: TaskAttemptContext
                         ): FileCommitProtocol.TaskCommitMessage = {
    val tPaths = taskFiles(taskContext.getTaskAttemptID)
    val dFiles = tPaths.map {
      case TaskPaths(taskPath, finalPath) =>
        DataFileUtils.buildDataFileFormPath(
          taskPath, iceSchema, partitionSchema,
          icePartSpec, iceFmt,
          taskContext.getConfiguration,
          finalPath.toString
        )
    }

    val dsgMsg = designate.commitTask(taskContext)
    new FileCommitProtocol.TaskCommitMessage(
      IcebergTaskCommitMessage(dsgMsg.obj, dFiles)
    )
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit =
    designate.abortTask(taskContext)

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    false
  }
}

object IcebergFileCommitProtocol {
  val REAL_FILE_COMMIT_PROTOCOL_CLASS = "spark.sql.sources.real.commitProtocolClass"

  case class TaskPaths(taskPath: String, finalPath: Path)

  case class IcebergTaskCommitMessage(
                                       designatePayload: Any,
                                       dataFiles: Seq[DataFile]
                                     )

}
