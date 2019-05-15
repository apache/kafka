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
import java.io.File

import kafka.server.LogDirFailureChannel
import org.apache.kafka.common.TopicPartition
import scala.collection._

/**
  * This class persists a map of (Partition => Offsets) to a file (for a certain replica)
  */
class OffsetCheckpointFile(val checkpointFile: CheckpointFile[OffsetCheckpointFileEntry]) extends AnyVal {
  def file: File = checkpointFile.file

  @deprecated("prefer using write(Seq[OffsetCheckpointFileEntry]) to avoid intermediate allocation", "2.2.0")
  def write(entries: Map[TopicPartition, Long]): Unit = {
    val converted: Seq[OffsetCheckpointFileEntry] = entries.map({case (tp: TopicPartition, offset: Long) => new OffsetCheckpointFileEntry(tp, offset)})(collection.breakOut)
    write(converted)
  }

  def write(entries: Seq[OffsetCheckpointFileEntry]): Unit = {
    checkpointFile.write(entries)
  }

  def read(): Map[TopicPartition, Long] = {
    val entries = checkpointFile.read()
    entries.map(e => e.topicPartition -> e.offset)(collection.breakOut)
  }
}

object OffsetCheckpointFile {
  val CurrentVersion = 0
  def apply(file: File, logDirFailureChannel: LogDirFailureChannel = null): OffsetCheckpointFile = {
    val checkpointFile = new CheckpointFile[OffsetCheckpointFileEntry](file, CurrentVersion, logDirFailureChannel, file.getParent, (line: String) => OffsetCheckpointFileEntry(line))
    new OffsetCheckpointFile(checkpointFile)
  }
}