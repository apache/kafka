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

package kafka.consumer

import kafka.api.{OffsetRequest, Request, FetchRequestBuilder, FetchResponsePartitionData}
import kafka.cluster.BrokerEndPoint
import kafka.message.ByteBufferMessageSet
import kafka.server.{PartitionFetchState, AbstractFetcherThread}
import kafka.common.{ErrorMapping, TopicAndPartition}
import scala.collection.Map
import ConsumerFetcherThread._
import org.apache.kafka.common.TopicPartition

class ConsumerFetcherThread(name: String,
                            val config: ConsumerConfig,
                            sourceBroker: BrokerEndPoint,
                            partitionMap: Map[TopicPartition, PartitionTopicInfo],
                            val consumerFetcherManager: ConsumerFetcherManager)
        extends AbstractFetcherThread(name = name,
                                      clientId = config.clientId,
                                      sourceBroker = sourceBroker,
                                      fetchBackOffMs = config.refreshLeaderBackoffMs,
                                      isInterruptible = true) {

  type REQ = FetchRequest
  type PD = PartitionData

  private val clientId = config.clientId
  private val fetchSize = config.fetchMessageMaxBytes

  private val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, config.socketTimeoutMs,
    config.socketReceiveBufferBytes, config.clientId)

  private val fetchRequestBuilder = new FetchRequestBuilder().
    clientId(clientId).
    replicaId(Request.OrdinaryConsumerId).
    maxWait(config.fetchWaitMaxMs).
    minBytes(config.fetchMinBytes).
    requestVersion(kafka.api.FetchRequest.CurrentVersion)

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown && isInterruptible)
      simpleConsumer.disconnectToHandleJavaIOBug()
    justShutdown
  }

  override def shutdown(): Unit = {
    super.shutdown()
    simpleConsumer.close()
  }

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
    val pti = partitionMap(topicPartition)
    if (pti.getFetchOffset != fetchOffset)
      throw new RuntimeException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d"
                                .format(topicPartition.topic, topicPartition.partition, pti.getFetchOffset, fetchOffset))
    pti.enqueue(partitionData.underlying.messages.asInstanceOf[ByteBufferMessageSet])
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = {
    val startTimestamp = config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => OffsetRequest.LatestTime
      case _ => OffsetRequest.LatestTime
    }
    val topicAndPartition = new TopicAndPartition(topicPartition.topic, topicPartition.partition)
    val newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId)
    val pti = partitionMap(topicPartition)
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) {
    removePartitions(partitions.toSet)
    consumerFetcherManager.addPartitionsWithError(partitions)
  }

  protected def buildFetchRequest(partitionMap: collection.Seq[(TopicPartition, PartitionFetchState)]): FetchRequest = {
    partitionMap.foreach { case ((topicPartition, partitionFetchState)) =>
      if (partitionFetchState.isActive)
        fetchRequestBuilder.addFetch(topicPartition.topic, topicPartition.partition, partitionFetchState.offset,
          fetchSize)
    }

    new FetchRequest(fetchRequestBuilder.build())
  }

  protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] =
    simpleConsumer.fetch(fetchRequest.underlying).data.map { case (TopicAndPartition(t, p), value) =>
      new TopicPartition(t, p) -> new PartitionData(value)
    }
}

object ConsumerFetcherThread {

  class FetchRequest(val underlying: kafka.api.FetchRequest) extends AbstractFetcherThread.FetchRequest {
    private lazy val tpToOffset: Map[TopicPartition, Long] = underlying.requestInfo.map { case (tp, fetchInfo) =>
      new TopicPartition(tp.topic, tp.partition) -> fetchInfo.offset
    }.toMap
    def isEmpty: Boolean = underlying.requestInfo.isEmpty
    def offset(topicPartition: TopicPartition): Long = tpToOffset(topicPartition)
  }

  class PartitionData(val underlying: FetchResponsePartitionData) extends AbstractFetcherThread.PartitionData {
    def errorCode: Short = underlying.error
    def toByteBufferMessageSet: ByteBufferMessageSet = underlying.messages.asInstanceOf[ByteBufferMessageSet]
    def highWatermark: Long = underlying.hw
    def exception: Option[Throwable] =
      if (errorCode == ErrorMapping.NoError) None else Some(ErrorMapping.exceptionFor(errorCode))

  }
}
