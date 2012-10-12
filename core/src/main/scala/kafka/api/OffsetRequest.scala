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
import kafka.utils.Utils
import kafka.common.TopicAndPartition


object OffsetRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""

  val SmallestTimeString = "smallest"
  val LargestTimeString = "largest"
  val LatestTime = -1L
  val EarliestTime = -2L

  def readFrom(buffer: ByteBuffer): OffsetRequest = {
    val versionId = buffer.getShort
    val clientId = Utils.readShortString(buffer)
    val replicaId = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = Utils.readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val time = buffer.getLong
        val maxNumOffsets = buffer.getInt
        (TopicAndPartition(topic, partitionId), PartitionOffsetRequestInfo(time, maxNumOffsets))
      })
    })
    OffsetRequest(Map(pairs:_*), versionId = versionId, clientId = clientId, replicaId = replicaId)
  }
}

case class PartitionOffsetRequestInfo(time: Long, maxNumOffsets: Int)

case class OffsetRequest(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo],
                         versionId: Short = OffsetRequest.CurrentVersion,
                         clientId: String = OffsetRequest.DefaultClientId,
                         replicaId: Int = Request.OrdinaryConsumerId)
        extends RequestOrResponse(Some(RequestKeys.OffsetsKey)) {

  def this(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo], replicaId: Int) = this(requestInfo, OffsetRequest.CurrentVersion, OffsetRequest.DefaultClientId, replicaId)

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    Utils.writeShortString(buffer, clientId)
    buffer.putInt(replicaId)

    buffer.putInt(requestInfoGroupedByTopic.size) // topic count
    requestInfoGroupedByTopic.foreach {
      case((topic, partitionInfos)) =>
        Utils.writeShortString(buffer, topic)
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
    Utils.shortStringLength(clientId, RequestOrResponse.DefaultCharset) +
    4 + /* replicaId */
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      val (topic, partitionInfos) = currTopic
      foldedTopics +
      Utils.shortStringLength(topic, RequestOrResponse.DefaultCharset) +
      4 + /* partition count */
      partitionInfos.size * (
        4 + /* partition */
        8 + /* time */
        4 /* maxNumOffsets */
      )
    })

  def isFromOrdinaryClient = replicaId == Request.OrdinaryConsumerId
  def isFromDebuggingClient = replicaId == Request.DebuggingConsumerId
}
