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
import kafka.common.TopicAndPartition
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response
import org.apache.kafka.common.protocol.{ApiKeys, Errors}


object OffsetRequest {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""

  val SmallestTimeString = "smallest"
  val LargestTimeString = "largest"
  val LatestTime = -1L
  val EarliestTime = -2L

  def readFrom(buffer: ByteBuffer): OffsetRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val replicaId = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val time = buffer.getLong
        val maxNumOffsets = buffer.getInt
        (TopicAndPartition(topic, partitionId), PartitionOffsetRequestInfo(time, maxNumOffsets))
      })
    })
    OffsetRequest(Map(pairs:_*), versionId= versionId, clientId = clientId, correlationId = correlationId, replicaId = replicaId)
  }
}

case class PartitionOffsetRequestInfo(time: Long, maxNumOffsets: Int)

case class OffsetRequest(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo],
                         versionId: Short = OffsetRequest.CurrentVersion,
                         correlationId: Int = 0,
                         clientId: String = OffsetRequest.DefaultClientId,
                         replicaId: Int = Request.OrdinaryConsumerId)
    extends RequestOrResponse(Some(ApiKeys.LIST_OFFSETS.id)) {

  def this(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo], correlationId: Int, replicaId: Int) = this(requestInfo, OffsetRequest.CurrentVersion, correlationId, OffsetRequest.DefaultClientId, replicaId)

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(replicaId)

    buffer.putInt(requestInfoGroupedByTopic.size) // topic count
    requestInfoGroupedByTopic.foreach {
      case((topic, partitionInfos)) =>
        writeShortString(buffer, topic)
        buffer.putInt(partitionInfos.size) // partition count
        partitionInfos.foreach {
          case (TopicAndPartition(_, partition), partitionInfo) =>
            buffer.putInt(partition)
            buffer.putLong(partitionInfo.time)
            buffer.putInt(partitionInfo.maxNumOffsets)
        }
    }
  }

  def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    4 + /* replicaId */
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      val (topic, partitionInfos) = currTopic
      foldedTopics +
      shortStringLength(topic) +
      4 + /* partition count */
      partitionInfos.size * (
        4 + /* partition */
        8 + /* time */
        4 /* maxNumOffsets */
      )
    })

  def isFromOrdinaryClient = replicaId == Request.OrdinaryConsumerId
  def isFromDebuggingClient = replicaId == Request.DebuggingConsumerId

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val partitionOffsetResponseMap = requestInfo.map {
      case (topicAndPartition, partitionOffsetRequest) =>
        (topicAndPartition, PartitionOffsetsResponse(Errors.forException(e).code, Nil))
    }
    val errorResponse = OffsetResponse(correlationId, partitionOffsetResponseMap)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val offsetRequest = new StringBuilder
    offsetRequest.append("Name: " + this.getClass.getSimpleName)
    offsetRequest.append("; Version: " + versionId)
    offsetRequest.append("; CorrelationId: " + correlationId)
    offsetRequest.append("; ClientId: " + clientId)
    offsetRequest.append("; ReplicaId: " + replicaId)
    if(details)
      offsetRequest.append("; RequestInfo: " + requestInfo.mkString(","))
    offsetRequest.toString()
  }
}