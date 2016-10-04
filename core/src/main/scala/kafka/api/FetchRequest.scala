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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class PartitionFetchInfo(offset: Long, fetchSize: Int)

object FetchRequest {

  private val random = new Random

  val CurrentVersion = 3.shortValue
  val DefaultMaxWait = 0
  val DefaultMinBytes = 0
  val DefaultMaxBytes = Int.MaxValue
  val DefaultCorrelationId = 0

  def readFrom(buffer: ByteBuffer): FetchRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val replicaId = buffer.getInt
    val maxWait = buffer.getInt
    val minBytes = buffer.getInt
    val maxBytes = if (versionId < 3) DefaultMaxBytes else buffer.getInt
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
    FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, maxBytes, Vector(pairs:_*))
  }

  def shuffle(requestInfo: Seq[(TopicAndPartition, PartitionFetchInfo)]): Seq[(TopicAndPartition, PartitionFetchInfo)] = {
    val groupedByTopic = requestInfo.groupBy { case (tp, _) => tp.topic }.map { case (topic, values) =>
      topic -> random.shuffle(values)
    }
    random.shuffle(groupedByTopic.toSeq).flatMap { case (topic, partitions) =>
      partitions.map { case (tp, fetchInfo) => tp -> fetchInfo }
    }
  }

  def batchByTopic[T](s: Seq[(TopicAndPartition, T)]): Seq[(String, Seq[(Int, T)])] = {
    val result = new ArrayBuffer[(String, ArrayBuffer[(Int, T)])]
    s.foreach { case (TopicAndPartition(t, p), value) =>
      if (result.isEmpty || result.last._1 != t)
        result += (t -> new ArrayBuffer)
      result.last._2 += (p -> value)
    }
    result
  }

}

case class FetchRequest(versionId: Short = FetchRequest.CurrentVersion,
                        correlationId: Int = FetchRequest.DefaultCorrelationId,
                        clientId: String = ConsumerConfig.DefaultClientId,
                        replicaId: Int = Request.OrdinaryConsumerId,
                        maxWait: Int = FetchRequest.DefaultMaxWait,
                        minBytes: Int = FetchRequest.DefaultMinBytes,
                        maxBytes: Int = FetchRequest.DefaultMaxBytes,
                        requestInfo: Seq[(TopicAndPartition, PartitionFetchInfo)])
        extends RequestOrResponse(Some(ApiKeys.FETCH.id)) {

  /**
    * Partitions the request info into a list of lists (one for each topic) while preserving request info ordering
    */
  private type PartitionInfos = Seq[(Int, PartitionFetchInfo)]
  private lazy val requestInfoGroupedByTopic: Seq[(String, PartitionInfos)] = FetchRequest.batchByTopic(requestInfo)

  /** Public constructor for the clients */
  @deprecated("The order of partitions in `requestInfo` is relevant, so this constructor is deprecated in favour of the " +
    "one that takes a Seq", since = "0.10.1.0")
  def this(correlationId: Int,
           clientId: String,
           maxWait: Int,
           minBytes: Int,
           maxBytes: Int,
           requestInfo: Map[TopicAndPartition, PartitionFetchInfo]) {
    this(versionId = FetchRequest.CurrentVersion,
         correlationId = correlationId,
         clientId = clientId,
         replicaId = Request.OrdinaryConsumerId,
         maxWait = maxWait,
         minBytes = minBytes,
         maxBytes = maxBytes,
         requestInfo = FetchRequest.shuffle(requestInfo.toSeq))
  }

  /** Public constructor for the clients */
  def this(correlationId: Int,
           clientId: String,
           maxWait: Int,
           minBytes: Int,
           maxBytes: Int,
           requestInfo: Seq[(TopicAndPartition, PartitionFetchInfo)]) {
    this(versionId = FetchRequest.CurrentVersion,
      correlationId = correlationId,
      clientId = clientId,
      replicaId = Request.OrdinaryConsumerId,
      maxWait = maxWait,
      minBytes = minBytes,
      maxBytes = maxBytes,
      requestInfo = requestInfo)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(replicaId)
    buffer.putInt(maxWait)
    buffer.putInt(minBytes)
    if (versionId >= 3)
      buffer.putInt(maxBytes)
    buffer.putInt(requestInfoGroupedByTopic.size) // topic count
    requestInfoGroupedByTopic.foreach {
      case (topic, partitionFetchInfos) =>
        writeShortString(buffer, topic)
        buffer.putInt(partitionFetchInfos.size) // partition count
        partitionFetchInfos.foreach {
          case (partition, PartitionFetchInfo(offset, fetchSize)) =>
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
    (if (versionId >= 3) 4 /* maxBytes */ else 0) +
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

  override def toString: String = {
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
    fetchRequest.append("; MaxBytes:" + maxBytes + " bytes")
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
  private var maxBytes = FetchRequest.DefaultMaxBytes
  private val requestMap = new collection.mutable.ArrayBuffer[(TopicAndPartition, PartitionFetchInfo)]

  def addFetch(topic: String, partition: Int, offset: Long, fetchSize: Int) = {
    requestMap.append((TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)))
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

  def maxBytes(maxBytes: Int): FetchRequestBuilder = {
    this.maxBytes = maxBytes
    this
  }

  def requestVersion(versionId: Short): FetchRequestBuilder = {
    this.versionId = versionId
    this
  }

  def build() = {
    val fetchRequest = FetchRequest(versionId, correlationId.getAndIncrement, clientId, replicaId, maxWait, minBytes,
      maxBytes, new ArrayBuffer() ++ requestMap)
    requestMap.clear()
    fetchRequest
  }
}
