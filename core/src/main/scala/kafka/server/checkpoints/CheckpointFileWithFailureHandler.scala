/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server.checkpoints

import kafka.server.LogDirFailureChannel
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.server.common.CheckpointFile
import CheckpointFile.EntryFormatter

import java.io._
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class CheckpointFileWithFailureHandler[T](val file: File,
                                          version: Int,
                                          formatter: EntryFormatter[T],
                                          logDirFailureChannel: LogDirFailureChannel,
                                          logDir: String) {
  private val checkpointFile = new CheckpointFile[T](file, version, formatter)

  def write(entries: Iterable[T]): Unit = {
      try {
        checkpointFile.write(entries.toSeq.asJava)
      } catch {
        case e: IOException =>
          val msg = s"Error while writing to checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
  }

  def read(): Seq[T] = {
      try {
        checkpointFile.read().asScala
      } catch {
        case e: IOException =>
          val msg = s"Error while reading checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
  }
}
