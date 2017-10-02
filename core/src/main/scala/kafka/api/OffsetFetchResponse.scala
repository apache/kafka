/**
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

package kafka.api

import java.nio.ByteBuffer

import kafka.api.ApiUtils._
import kafka.common.{TopicAndPartition, OffsetMetadataAndError}
import kafka.utils.Logging

import org.apache.kafka.common.protocol.Errors

@deprecated("This object has been deprecated and will be removed in a future release.", "1.0.0")
object OffsetFetchResponse extends Logging {

  def readFrom(buffer: ByteBuffer): OffsetFetchResponse = {
    readFrom(buffer, OffsetFetchRequest.CurrentVersion)
  }

  def readFrom(buffer: ByteBuffer, requestVersion: Int): OffsetFetchResponse = {
    val correlationId = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val metadata = readShortString(buffer)
        val error = Errors.forCode(buffer.getShort)
        (TopicAndPartition(topic, partitionId), OffsetMetadataAndError(offset, metadata, error))
      })
    })

    val error = requestVersion match {
      case 0 | 1 => Errors.NONE
      case _ => Errors.forCode(buffer.getShort)
    }

    OffsetFetchResponse(Map(pairs:_*), requestVersion, correlationId, error)
  }
}

@deprecated("This object has been deprecated and will be removed in a future release.", "1.0.0")
case class OffsetFetchResponse(requestInfo: Map[TopicAndPartition, OffsetMetadataAndError],
                               requestVersion: Int = OffsetFetchRequest.CurrentVersion,
                               correlationId: Int = 0,
                               error: Errors = Errors.NONE)
    extends RequestOrResponse() {

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(correlationId)
    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics
    requestInfoGroupedByTopic.foreach( t1 => { // topic -> Map[TopicAndPartition, OffsetMetadataAndError]
      writeShortString(buffer, t1._1) // topic
      buffer.putInt(t1._2.size)       // number of partitions for this topic
      t1._2.foreach( t2 => { // TopicAndPartition -> OffsetMetadataAndError
        buffer.putInt(t2._1.partition)
        buffer.putLong(t2._2.offset)
        writeShortString(buffer, t2._2.metadata)
        buffer.putShort(t2._2.error.code)
      })
    })

    // the top level error_code was introduced in v2
    if (requestVersion > 1)
      buffer.putShort(error.code)
  }

  override def sizeInBytes =
    4 + /* correlationId */
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((count, topicAndOffsets) => {
      val (topic, offsets) = topicAndOffsets
      count +
      shortStringLength(topic) + /* topic */
      4 + /* number of partitions */
      offsets.foldLeft(0)((innerCount, offsetsAndMetadata) => {
        innerCount +
        4 /* partition */ +
        8 /* offset */ +
        shortStringLength(offsetsAndMetadata._2.metadata) +
        2 /* error */
      })
    }) +
    (if (requestVersion > 1) 2 else 0) /* error */

  override def describe(details: Boolean):String = { toString }
}

