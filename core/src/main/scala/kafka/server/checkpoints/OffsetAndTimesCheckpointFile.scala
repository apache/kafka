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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition

import scala.collection._

/*
 * Will be used only to store offset and times as we have 
 * adopted a new file system where each partition has their
 * own unique checkpoint file.
 */
object OffsetAndTimesCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private[checkpoints] val CurrentVersion = 0

  object Formatter extends CheckpointFileFormatter[OffsetAndTimestamp] {
    override def toLine(entry: OffsetAndTimestamp): String = {
      s"${entry.offset()} ${entry.timestamp()}"
    }

    override def fromLine(line: String): Option[OffsetAndTimestamp] = {
      WhiteSpacesPattern.split(line) match {
        case Array(offset, time) =>
          Some(new OffsetAndTimestamp(offset.toLong, time.toLong))
        case _ => None
      }
    }
  }
}

/**
  * This class persists a collection of OffsetAndTime to a file (for a certain topic)
  */
class OffsetAndTimesCheckpointFile(val file: File, val partition: TopicPartition, logDirFailureChannel: LogDirFailureChannel = null) {
  val checkpoint = new CheckpointFile[OffsetAndTimestamp](file, OffsetAndTimesCheckpointFile.CurrentVersion,
    OffsetAndTimesCheckpointFile.Formatter, logDirFailureChannel, file.getParent)

  def write(offset: OffsetAndTimestamp): Unit = {
    if (offset == null) {
      checkpoint.write(Seq())
    } else {
      checkpoint.write(Seq(offset))
    }
  }

  def read(): OffsetAndTimestamp = readSeq()(0)

  def readSeq(): Seq[OffsetAndTimestamp] = checkpoint.read()
}

trait OffsetAndTimeCheckpoints {
  def fetch(logDir: String): OffsetAndTimestamp
}

/**
 * Loads checkpoint files on demand and caches the offsets for reuse.
 */
class LazyOffsetAndTimesCheckpoints(checkpointsByLogDir: Map[String, OffsetAndTimesCheckpointFile]) extends OffsetAndTimeCheckpoints {
  private val lazyCheckpointsByLogDir = checkpointsByLogDir.map { case (logDir, checkpointFile) =>
    logDir -> new LazyOffsetAndTimesCheckpointMap(checkpointFile)
  }.toMap

  override def fetch(logDir: String): OffsetAndTimestamp = {
    val offsetCheckpointFile = lazyCheckpointsByLogDir.getOrElse(logDir,
      throw new IllegalArgumentException(s"No checkpoint file for log dir $logDir"))
    offsetCheckpointFile.fetch()
  }
}

class LazyOffsetAndTimesCheckpointMap(checkpoint: OffsetAndTimesCheckpointFile) {
  private lazy val offset: OffsetAndTimestamp = checkpoint.read()

  def fetch() : OffsetAndTimestamp = {
    offset
  }
}
