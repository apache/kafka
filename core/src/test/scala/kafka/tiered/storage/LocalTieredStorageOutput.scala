/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.tiered.storage

import java.nio.ByteBuffer

import kafka.utils.nonthreadsafe
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.SEGMENT
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageTraverser, RemoteLogSegmentFileset}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.Utils

import scala.jdk.CollectionConverters._

@nonthreadsafe
final class LocalTieredStorageOutput[K, V](val keyDe: Deserializer[K],
                                           val valueDe: Deserializer[V]) extends LocalTieredStorageTraverser {
  private[storage] var output: String =
    row("File", "Offsets", "Records", "Broker ID")

  output += "-" * (51 + 8 + 13 + 10 + (3 * 2)) + "\n" // Columns length + 5 column separators.

  private def row(file: String = "", offset: Any = "", record: String = "", ident: String = " " * 4) = {
    f"${ident + file}%-51s |${offset.toString}%8s |$record%13s \n"
  }

  private var currentTopic: String = ""

  override def visitTopicPartition(topicPartition: TopicPartition): Unit = {
    currentTopic = topicPartition.topic()
    output += row(topicPartition.toString, ident = "")
  }

  override def visitSegment(fileset: RemoteLogSegmentFileset): Unit = {
    def des(de: Deserializer[_])(bytes: ByteBuffer): String = {
      de.deserialize(currentTopic, Utils.toNullableArray(bytes)).toString
    }

    val records = fileset.getRecords.asScala.toList
    val segFilename = fileset.getFile(SEGMENT).getName

    if (records.isEmpty) {
      output += row(segFilename, -1, "")

    } else {
      val keyValues = records
        .map(record => (record.offset(), des(keyDe)(record.key()), des(valueDe)(record.value())))
        .map(offsetKeyValue => (offsetKeyValue._1, s"(${offsetKeyValue._2}, ${offsetKeyValue._3})"))
        .splitAt(1)

      keyValues._1.foreach { case (offset, kv) => { output += row(segFilename, offset, kv) }}
      keyValues._2.foreach { case (offset, kv) => { output += row("", offset, kv) }}
    }

    output += row()
  }
}

object DumpLocalTieredStorage {

  def dump[K, V](storage: LocalTieredStorage, keyDe: Deserializer[K], valueDe: Deserializer[V]): String = {
    val output = new LocalTieredStorageOutput(keyDe, valueDe)
    storage.traverse(output)
    output.output
  }

}
