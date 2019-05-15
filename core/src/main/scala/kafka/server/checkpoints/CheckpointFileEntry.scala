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
package kafka.server.checkpoints

import org.apache.kafka.common.TopicPartition
import java.util
import java.io.IOException

trait CheckpointFileEntry {
  /**
    * Encodes this CheckpointFileEntry as a line of text for the given StringBuilder.
    */
  def writeLine(stringBuilder: StringBuilder): Unit
}

final case class OffsetCheckpointFileEntry(topicPartition: TopicPartition, offset: Long) extends CheckpointFileEntry() {
  /**
    * Encodes this CheckpointFileEntry as a line of text for the given StringBuilder.
    */
  override def writeLine(stringBuilder: StringBuilder): Unit = {
    stringBuilder
      .append(topicPartition.topic)
      .append(' ')
      .append(topicPartition.partition)
      .append(' ')
      .append(offset)
  }
}

object OffsetCheckpointFileEntry {
  private val SEPARATOR: util.regex.Pattern = util.regex.Pattern.compile("\\s")
  def apply(line: String): OffsetCheckpointFileEntry = {
    SEPARATOR.split(line) match {
      case Array(topic, partition, offset) => OffsetCheckpointFileEntry(new TopicPartition(topic, partition.toInt), offset.toLong)
      case _ =>
        throw new IOException(s"Expected 3 elements for OffsetCheckpointFileEntry")
    }
  }
}
