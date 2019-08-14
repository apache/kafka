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

class OffsetAndTime(val offset: Long, val time: Long)

/*
 * Will be used only to store offset and times as we have 
 * adopted a new file system where each partition has their
 * own unique checkpoint file.
 */
object OffsetAndTimesCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private[checkpoints] val CurrentVersion = 0

  object Formatter extends CheckpointFileFormatter[OffsetAndTime] {
    override def toLine(entry: OffsetAndTime): String = {
      s"${entry.offset} ${entry.time}"
    }

    override def fromLine(line: String): Option[OffsetAndTime] = {
      WhiteSpacesPattern.split(line) match {
        case Array(offset, time) =>
          Some(new OffsetAndTime(offset.toLong, time.toLong))
        case _ => None
      }
    }
  }
}

/**
  * This class persists a map of (Partition => Offsets) to a file (for a certain replica)
  */
class OffsetAndTimesCheckpointFile(val file: File, logDirFailureChannel: LogDirFailureChannel = null) {
  val checkpoint = new CheckpointFile[OffsetAndTime](file, OffsetAndTimesCheckpointFile.CurrentVersion,
    OffsetAndTimesCheckpointFile.Formatter, logDirFailureChannel, file.getParent)

  def write(offsets: Seq[OffsetAndTime]): Unit = checkpoint.write(offsets)

  def read(): Seq[OffsetAndTime] = checkpoint.read()

}

trait OffsetAndTimeCheckpoints {
  def fetch(logDir: String, topicPartition: TopicPartition): Seq[OffsetAndTime]
}

/**
 * Loads checkpoint files on demand and caches the offsets for reuse.
 */
class LazyOffsetAndTimesCheckpoints(checkpointsByLogDir: Map[String, OffsetAndTimesCheckpointFile]) extends OffsetAndTimeCheckpoints {
  private val lazyCheckpointsByLogDir = checkpointsByLogDir.map { case (logDir, checkpointFile) =>
    logDir -> new LazyOffsetAndTimesCheckpointMap(checkpointFile)
  }.toMap

  override def fetch(logDir: String, topicPartition: TopicPartition): Seq[OffsetAndTime] = {
    val offsetCheckpointFile = lazyCheckpointsByLogDir.getOrElse(logDir,
      throw new IllegalArgumentException(s"No checkpoint file for log dir $logDir"))
    offsetCheckpointFile.fetch()
  }
}

class LazyOffsetAndTimesCheckpointMap(checkpoint: OffsetAndTimesCheckpointFile) {
  private lazy val offsets: Seq[OffsetAndTime] = checkpoint.read()

  def fetch() : Seq[OffsetAndTime] = {
    offsets
  }

}
