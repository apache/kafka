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

import java.io._
import java.util.regex.Pattern

import kafka.server.LogDirFailureChannel
import kafka.server.epoch.EpochEntry
import org.apache.kafka.common.TopicPartition

import scala.collection._

object OffsetCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private[checkpoints] val CurrentVersion = 0

  object Formatter extends CheckpointFileFormatter[(TopicPartition, Long)] {
    override def toLine(entry: (TopicPartition, Long)): String = {
      s"${entry._1.topic} ${entry._1.partition} ${entry._2}"
    }

    override def fromLine(line: String): Option[(TopicPartition, Long)] = {
      WhiteSpacesPattern.split(line) match {
        case Array(topic, partition, offset) =>
          Some(new TopicPartition(topic, partition.toInt), offset.toLong)
        case _ => None
      }
    }
  }
}

trait OffsetCheckpoint {
  def write(epochs: Seq[EpochEntry])
  def read(): Seq[EpochEntry]
}

/**
  * This class persists a map of (Partition => Offsets) to a file (for a certain replica)
  */
class OffsetCheckpointFile(val file: File, logDirFailureChannel: LogDirFailureChannel = null) {
  val checkpoint = new CheckpointFile[(TopicPartition, Long)](file, OffsetCheckpointFile.CurrentVersion,
    OffsetCheckpointFile.Formatter, logDirFailureChannel, file.getParent)

  def write(offsets: Map[TopicPartition, Long]): Unit = checkpoint.write(offsets.toSeq)

  def read(): Map[TopicPartition, Long] = checkpoint.read().toMap

}
