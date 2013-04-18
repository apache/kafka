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
import kafka.utils.Logging
import kafka.network.{RequestChannel, BoundedByteBufferSend}
import kafka.common.{ErrorMapping, TopicAndPartition, OffsetMetadataAndError}
import kafka.network.RequestChannel.Response
object OffsetCommitRequest extends Logging {
  val CurrentVersion: Short = 0
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): OffsetCommitRequest = {
    // Read values from the envelope
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)

    // Read the OffsetRequest 
    val consumerGroupId = readShortString(buffer)
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val metadata = readShortString(buffer)
        (TopicAndPartition(topic, partitionId), OffsetMetadataAndError(offset, metadata))
      })
    })
    OffsetCommitRequest(consumerGroupId, Map(pairs:_*), versionId, correlationId, clientId)
  }
}

case class OffsetCommitRequest(groupId: String,
                               requestInfo: Map[TopicAndPartition, OffsetMetadataAndError],
                               versionId: Short = OffsetCommitRequest.CurrentVersion,
                               override val correlationId: Int = 0,
                               clientId: String = OffsetCommitRequest.DefaultClientId)
    extends RequestOrResponse(Some(RequestKeys.OffsetCommitKey), correlationId) {

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)
  
  def writeTo(buffer: ByteBuffer) {
    // Write envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)

    // Write OffsetCommitRequest
    writeShortString(buffer, groupId)             // consumer group
    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics
    requestInfoGroupedByTopic.foreach( t1 => { // topic -> Map[TopicAndPartition, OffsetMetadataAndError]
      writeShortString(buffer, t1._1) // topic
      buffer.putInt(t1._2.size)       // number of partitions for this topic
      t1._2.foreach( t2 => {
        buffer.putInt(t2._1.partition)  // partition
        buffer.putLong(t2._2.offset)    // offset
        writeShortString(buffer, t2._2.metadata) // metadata
      })
    })
  }

  override def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    shortStringLength(groupId) + 
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((count, topicAndOffsets) => {
      val (topic, offsets) = topicAndOffsets
      count +
      shortStringLength(topic) + /* topic */
      4 + /* number of partitions */
      offsets.foldLeft(0)((innerCount, offsetAndMetadata) => {
        innerCount +
        4 /* partition */ +
        8 /* offset */ +
        shortStringLength(offsetAndMetadata._2.metadata)
      })
    })

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val responseMap = requestInfo.map {
      case (topicAndPartition, offset) => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }.toMap
    val errorResponse = OffsetCommitResponse(requestInfo=responseMap, correlationId=correlationId)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }
}
