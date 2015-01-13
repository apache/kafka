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
import kafka.utils.{SystemTime, Logging}
import kafka.network.{RequestChannel, BoundedByteBufferSend}
import kafka.common.{OffsetAndMetadata, ErrorMapping, TopicAndPartition}
import kafka.network.RequestChannel.Response
import scala.collection._

object OffsetCommitRequest extends Logging {
  val CurrentVersion: Short = 1
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): OffsetCommitRequest = {
    // Read values from the envelope
    val versionId = buffer.getShort
    assert(versionId == 0 || versionId == 1,
           "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0 or 1.")

    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)

    // Read the OffsetRequest 
    val consumerGroupId = readShortString(buffer)

    // version 1 specific fields
    var groupGenerationId: Int = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID
    var consumerId: String = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID
    if (versionId == 1) {
      groupGenerationId = buffer.getInt
      consumerId = readShortString(buffer)
    }

    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val timestamp = {
          if (versionId == 1) {
            val given = buffer.getLong
            given
          } else
            OffsetAndMetadata.InvalidTime
        }
        val metadata = readShortString(buffer)
        (TopicAndPartition(topic, partitionId), OffsetAndMetadata(offset, metadata, timestamp))
      })
    })
    OffsetCommitRequest(consumerGroupId, immutable.Map(pairs:_*), versionId, correlationId, clientId, groupGenerationId, consumerId)
  }

  def changeInvalidTimeToCurrentTime(offsetCommitRequest: OffsetCommitRequest) {
    val now = SystemTime.milliseconds
    for ( (topicAndPartiiton, offsetAndMetadata) <- offsetCommitRequest.requestInfo)
      if (offsetAndMetadata.timestamp == OffsetAndMetadata.InvalidTime)
        offsetAndMetadata.timestamp = now
  }
}

case class OffsetCommitRequest(groupId: String,
                               requestInfo: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                               versionId: Short = OffsetCommitRequest.CurrentVersion,
                               correlationId: Int = 0,
                               clientId: String = OffsetCommitRequest.DefaultClientId,
                               groupGenerationId: Int = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID,
                               consumerId: String =  org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID)
    extends RequestOrResponse(Some(RequestKeys.OffsetCommitKey)) {
  assert(versionId == 0 || versionId == 1,
         "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0 or 1.")

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  def filterLargeMetadata(maxMetadataSize: Int) =
    requestInfo.filter(info => info._2.metadata == null || info._2.metadata.length <= maxMetadataSize)

  def responseFor(errorCode: Short, offsetMetadataMaxSize: Int) = {
    val commitStatus = requestInfo.map {info =>
      (info._1, if (info._2.metadata != null && info._2.metadata.length > offsetMetadataMaxSize)
                  ErrorMapping.OffsetMetadataTooLargeCode
                else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode)
                  ErrorMapping.ConsumerCoordinatorNotAvailableCode
                else if (errorCode == ErrorMapping.NotLeaderForPartitionCode)
                  ErrorMapping.NotCoordinatorForConsumerCode
                else
                  errorCode)
    }.toMap
    OffsetCommitResponse(commitStatus, correlationId)
  }

  def writeTo(buffer: ByteBuffer) {
    // Write envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)

    // Write OffsetCommitRequest
    writeShortString(buffer, groupId)             // consumer group

    // version 1 specific data
    if (versionId == 1) {
      buffer.putInt(groupGenerationId)
      writeShortString(buffer, consumerId)
    }
    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics
    requestInfoGroupedByTopic.foreach( t1 => { // topic -> Map[TopicAndPartition, OffsetMetadataAndError]
      writeShortString(buffer, t1._1) // topic
      buffer.putInt(t1._2.size)       // number of partitions for this topic
      t1._2.foreach( t2 => {
        buffer.putInt(t2._1.partition)
        buffer.putLong(t2._2.offset)
        if (versionId == 1)
          buffer.putLong(t2._2.timestamp)
        writeShortString(buffer, t2._2.metadata)
      })
    })
  }

  override def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    shortStringLength(groupId) +
    (if (versionId == 1) 4 /* group generation id */ + shortStringLength(consumerId) else 0) +
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
        (if (versionId == 1) 8 else 0 ) /* timestamp */ +
        shortStringLength(offsetAndMetadata._2.metadata)
      })
    })

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])
    val errorResponse = responseFor(errorCode, Int.MaxValue)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val offsetCommitRequest = new StringBuilder
    offsetCommitRequest.append("Name: " + this.getClass.getSimpleName)
    offsetCommitRequest.append("; Version: " + versionId)
    offsetCommitRequest.append("; CorrelationId: " + correlationId)
    offsetCommitRequest.append("; ClientId: " + clientId)
    offsetCommitRequest.append("; GroupId: " + groupId)
    offsetCommitRequest.append("; GroupGenerationId: " + groupGenerationId)
    offsetCommitRequest.append("; ConsumerId: " + consumerId)
    if(details)
      offsetCommitRequest.append("; RequestInfo: " + requestInfo.mkString(","))
    offsetCommitRequest.toString()
  }

  override def toString = {
    describe(details = true)
  }
}
