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

import kafka.utils.nonthreadsafe
import kafka.api.ApiUtils._
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.network.RequestChannel
import kafka.message.MessageSet

import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

import scala.collection.immutable.Map

case class PartitionFetchInfo(offset: Long, fetchSize: Int)

object FetchRequest {
  val CurrentVersion = 2.shortValue
  val DefaultMaxWait = 0
  val DefaultMinBytes = 0
  val DefaultCorrelationId = 0

  def readFrom(buffer: ByteBuffer): FetchRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val replicaId = buffer.getInt
    val maxWait = buffer.getInt
    val minBytes = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
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
                        clientId: String = ConsumerConfig.DefaultClientId,
                        replicaId: Int = Request.OrdinaryConsumerId,
                        maxWait: Int = FetchRequest.DefaultMaxWait,
                        minBytes: Int = FetchRequest.DefaultMinBytes,
                        requestInfo: Map[TopicAndPartition, PartitionFetchInfo])
        extends RequestOrResponse(Some(ApiKeys.FETCH.id)) {

  /**
   * Partitions the request info into a map of maps (one for each topic).
   */
  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  /**
   *  Public constructor for the clients
   */
  def this(correlationId: Int,
           clientId: String,
           maxWait: Int,
           minBytes: Int,
           requestInfo: Map[TopicAndPartition, PartitionFetchInfo]) {
    this(versionId = FetchRequest.CurrentVersion,
         correlationId = correlationId,
         clientId = clientId,
         replicaId = Request.OrdinaryConsumerId,
         maxWait = maxWait,
         minBytes= minBytes,
         requestInfo = requestInfo)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(replicaId)
    buffer.putInt(maxWait)
    buffer.putInt(minBytes)
    buffer.putInt(requestInfoGroupedByTopic.size) // topic count
    requestInfoGroupedByTopic.foreach {
      case (topic, partitionFetchInfos) =>
        writeShortString(buffer, topic)
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
    shortStringLength(clientId) +
    4 + /* replicaId */
    4 + /* maxWait */
    4 + /* minBytes */
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      val (topic, partitionFetchInfos) = currTopic
      foldedTopics +
      shortStringLength(topic) +
      4 + /* partition count */
      partitionFetchInfos.size * (
        4 + /* partition id */
        8 + /* offset */
        4 /* fetch size */
      )
    })
  }

  def isFromFollower = Request.isValidBrokerId(replicaId)

  def isFromOrdinaryConsumer = replicaId == Request.OrdinaryConsumerId

  def isFromLowLevelConsumer = replicaId == Request.DebuggingConsumerId

  def numPartitions = requestInfo.size

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val fetchResponsePartitionData = requestInfo.map {
      case (topicAndPartition, data) =>
        (topicAndPartition, FetchResponsePartitionData(Errors.forException(e).code, -1, MessageSet.Empty))
    }
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]
    val errorResponse = FetchResponse(correlationId, fetchResponsePartitionData, fetchRequest.versionId)
    // Magic value does not matter here because the message set is empty
    requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(request.connectionId, errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val fetchRequest = new StringBuilder
    fetchRequest.append("Name: " + this.getClass.getSimpleName)
    fetchRequest.append("; Version: " + versionId)
    fetchRequest.append("; CorrelationId: " + correlationId)
    fetchRequest.append("; ClientId: " + clientId)
    fetchRequest.append("; ReplicaId: " + replicaId)
    fetchRequest.append("; MaxWait: " + maxWait + " ms")
    fetchRequest.append("; MinBytes: " + minBytes + " bytes")
    if(details)
      fetchRequest.append("; RequestInfo: " + requestInfo.mkString(","))
    fetchRequest.toString()
  }
}

@nonthreadsafe
class FetchRequestBuilder() {
  private val correlationId = new AtomicInteger(0)
  private var versionId = FetchRequest.CurrentVersion
  private var clientId = ConsumerConfig.DefaultClientId
  private var replicaId = Request.OrdinaryConsumerId
  private var maxWait = FetchRequest.DefaultMaxWait
  private var minBytes = FetchRequest.DefaultMinBytes
  private val requestMap = new collection.mutable.HashMap[TopicAndPartition, PartitionFetchInfo]

  def addFetch(topic: String, partition: Int, offset: Long, fetchSize: Int) = {
    requestMap.put(TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize))
    this
  }

  def clientId(clientId: String): FetchRequestBuilder = {
    this.clientId = clientId
    this
  }

  /**
   * Only for internal use. Clients shouldn't set replicaId.
   */
  private[kafka] def replicaId(replicaId: Int): FetchRequestBuilder = {
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

  def requestVersion(versionId: Short): FetchRequestBuilder = {
    this.versionId = versionId
    this
  }

  def build() = {
    val fetchRequest = FetchRequest(versionId, correlationId.getAndIncrement, clientId, replicaId, maxWait, minBytes, requestMap.toMap)
    requestMap.clear()
    fetchRequest
  }
}
