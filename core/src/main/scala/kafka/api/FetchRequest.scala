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
import kafka.utils.{nonthreadsafe, Utils}
import scala.collection.immutable.Map
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig


case class PartitionFetchInfo(offset: Long, fetchSize: Int)


object FetchRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultCorrelationId = -1
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): FetchRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = Utils.readShortString(buffer, RequestOrResponse.DefaultCharset)
    val replicaId = buffer.getInt
    val maxWait = buffer.getInt
    val minBytes = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = Utils.readShortString(buffer, RequestOrResponse.DefaultCharset)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val fetchSize = buffer.getInt
        (TopicAndPartition(topic, partitionId), PartitionFetchInfo(offset, fetchSize))
      })
    })
    FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, Map(pairs:_*))
  }
}

case class FetchRequest(versionId: Short = FetchRequest.CurrentVersion,
                        correlationId: Int = FetchRequest.DefaultCorrelationId,
                        clientId: String = FetchRequest.DefaultClientId,
                        replicaId: Int = Request.OrdinaryConsumerId,
                        maxWait: Int = ConsumerConfig.MaxFetchWaitMs,
                        minBytes: Int = ConsumerConfig.MinFetchBytes,
                        requestInfo: Map[TopicAndPartition, PartitionFetchInfo])
        extends RequestOrResponse(Some(RequestKeys.FetchKey)) {

  /**
   * Partitions the request info into a map of maps (one for each topic).
   */
  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    Utils.writeShortString(buffer, clientId, RequestOrResponse.DefaultCharset)
    buffer.putInt(replicaId)
    buffer.putInt(maxWait)
    buffer.putInt(minBytes)
    buffer.putInt(requestInfoGroupedByTopic.size) // topic count
    requestInfoGroupedByTopic.foreach {
      case (topic, partitionFetchInfos) =>
        Utils.writeShortString(buffer, topic, RequestOrResponse.DefaultCharset)
        buffer.putInt(partitionFetchInfos.size) // partition count
        partitionFetchInfos.foreach {
          case (TopicAndPartition(_, partition), PartitionFetchInfo(offset, fetchSize)) =>
            buffer.putInt(partition)
            buffer.putLong(offset)
            buffer.putInt(fetchSize)
        }
    }
  }

  def sizeInBytes: Int = {
    2 + /* versionId */
    4 + /* correlationId */
    Utils.shortStringLength(clientId, RequestOrResponse.DefaultCharset) +
    4 + /* replicaId */
    4 + /* maxWait */
    4 + /* minBytes */
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      val (topic, partitionFetchInfos) = currTopic
      foldedTopics +
      Utils.shortStringLength(topic, RequestOrResponse.DefaultCharset) +
      4 + /* partition count */
      partitionFetchInfos.size * (
        4 + /* partition id */
        8 + /* offset */
        4 /* fetch size */
      )
    })
  }

  def isFromFollower = replicaId != Request.OrdinaryConsumerId && replicaId != Request.DebuggingConsumerId

  def isFromOrdinaryConsumer = replicaId == Request.OrdinaryConsumerId

  def isFromLowLevelConsumer = replicaId == Request.DebuggingConsumerId

  def numPartitions = requestInfo.size
}


@nonthreadsafe
class FetchRequestBuilder() {
  private var correlationId = FetchRequest.DefaultCorrelationId
  private val versionId = FetchRequest.CurrentVersion
  private var clientId = FetchRequest.DefaultClientId
  private var replicaId = Request.OrdinaryConsumerId
  private var maxWait = ConsumerConfig.MaxFetchWaitMs
  private var minBytes = ConsumerConfig.MinFetchBytes
  private val requestMap = new collection.mutable.HashMap[TopicAndPartition, PartitionFetchInfo]

  def addFetch(topic: String, partition: Int, offset: Long, fetchSize: Int) = {
    requestMap.put(TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize))
    this
  }

  def correlationId(correlationId: Int): FetchRequestBuilder = {
    this.correlationId = correlationId
    this
  }

  def clientId(clientId: String): FetchRequestBuilder = {
    this.clientId = clientId
    this
  }

  def replicaId(replicaId: Int): FetchRequestBuilder = {
    this.replicaId = replicaId
    this
  }

  def maxWait(maxWait: Int): FetchRequestBuilder = {
    this.maxWait = maxWait
    this
  }

  def minBytes(minBytes: Int): FetchRequestBuilder = {
    this.minBytes = minBytes
    this
  }

  def build() = FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, requestMap.toMap)
}
