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
import kafka.server.epoch.EpochEntry
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.common.CheckpointFile.EntryFormatter

import java.io._
import java.util.Optional
import java.util.regex.Pattern
import scala.collection._

object OffsetCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private[checkpoints] val CurrentVersion = 0

  object Formatter extends EntryFormatter[(TopicPartition, Long)] {
    override def toString(entry: (TopicPartition, Long)): String = {
      s"${entry._1.topic} ${entry._1.partition} ${entry._2}"
    }

    override def fromString(line: String): Optional[(TopicPartition, Long)] = {
      WhiteSpacesPattern.split(line) match {
        case Array(topic, partition, offset) =>
          Optional.of(new TopicPartition(topic, partition.toInt), offset.toLong)
        case _ => Optional.empty()
      }
    }
  }
}

trait OffsetCheckpoint {
  def write(epochs: Seq[EpochEntry]): Unit
  def read(): Seq[EpochEntry]
}

/**
 * This class persists a map of (Partition => Offsets) to a file (for a certain replica)
 *
 * The format in the offset checkpoint file is like this:
 *  -----checkpoint file begin------
 *  0                <- OffsetCheckpointFile.currentVersion
 *  2                <- following entries size
 *  tp1  par1  1     <- the format is: TOPIC  PARTITION  OFFSET
 *  tp1  par2  2
 *  -----checkpoint file end----------
 */
class OffsetCheckpointFile(val file: File, logDirFailureChannel: LogDirFailureChannel = null) {
  val checkpoint = new CheckpointFileWithFailureHandler[(TopicPartition, Long)](file, OffsetCheckpointFile.CurrentVersion,
    OffsetCheckpointFile.Formatter, logDirFailureChannel, file.getParent)

  def write(offsets: Map[TopicPartition, Long]): Unit = checkpoint.write(offsets)

  def read(): Map[TopicPartition, Long] = checkpoint.read().toMap

}

trait OffsetCheckpoints {
  def fetch(logDir: String, topicPartition: TopicPartition): Option[Long]
}

/**
 * Loads checkpoint files on demand and caches the offsets for reuse.
 */
class LazyOffsetCheckpoints(checkpointsByLogDir: Map[String, OffsetCheckpointFile]) extends OffsetCheckpoints {
  private val lazyCheckpointsByLogDir = checkpointsByLogDir.map { case (logDir, checkpointFile) =>
    logDir -> new LazyOffsetCheckpointMap(checkpointFile)
  }.toMap

  override def fetch(logDir: String, topicPartition: TopicPartition): Option[Long] = {
    val offsetCheckpointFile = lazyCheckpointsByLogDir.getOrElse(logDir,
      throw new IllegalArgumentException(s"No checkpoint file for log dir $logDir"))
    offsetCheckpointFile.fetch(topicPartition)
  }
}

class LazyOffsetCheckpointMap(checkpoint: OffsetCheckpointFile) {
  private lazy val offsets: Map[TopicPartition, Long] = checkpoint.read()

  def fetch(topicPartition: TopicPartition): Option[Long] = {
    offsets.get(topicPartition)
  }

}
