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
import scala.collection.JavaConverters
import JavaConverters._
import ConsumerFetcherThread._

class ConsumerFetcherThread(name: String,
                            val config: ConsumerConfig,
                            sourceBroker: BrokerEndPoint,
                            partitionMap: Map[TopicAndPartition, PartitionTopicInfo],
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
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: PartitionData) {
    val pti = partitionMap(topicAndPartition)
    if (pti.getFetchOffset != fetchOffset)
      throw new RuntimeException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d"
                                .format(topicAndPartition.topic, topicAndPartition.partition, pti.getFetchOffset, fetchOffset))
    pti.enqueue(partitionData.underlying.messages.asInstanceOf[ByteBufferMessageSet])
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    val startTimestamp = config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => OffsetRequest.LatestTime
      case _ => OffsetRequest.LatestTime
    }
    val newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId)
    val pti = partitionMap(topicAndPartition)
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    removePartitions(partitions.toSet)
    consumerFetcherManager.addPartitionsWithError(partitions)
  }

  protected def buildFetchRequest(partitionMap: collection.Map[TopicAndPartition, PartitionFetchState]): FetchRequest = {
    partitionMap.foreach { case ((topicAndPartition, partitionFetchState)) =>
      if (partitionFetchState.isActive)
        fetchRequestBuilder.addFetch(topicAndPartition.topic, topicAndPartition.partition, partitionFetchState.offset,
          fetchSize)
    }

    new FetchRequest(fetchRequestBuilder.build())
  }

  protected def fetch(fetchRequest: FetchRequest): collection.Map[TopicAndPartition, PartitionData] =
    simpleConsumer.fetch(fetchRequest.underlying).data.map { case (key, value) =>
      key -> new PartitionData(value)
    }

}

object ConsumerFetcherThread {

  class FetchRequest(val underlying: kafka.api.FetchRequest) extends AbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.requestInfo.isEmpty
    def offset(topicAndPartition: TopicAndPartition): Long = underlying.requestInfo(topicAndPartition).offset
  }

  class PartitionData(val underlying: FetchResponsePartitionData) extends AbstractFetcherThread.PartitionData {
    def errorCode: Short = underlying.error
    def toByteBufferMessageSet: ByteBufferMessageSet = underlying.messages.asInstanceOf[ByteBufferMessageSet]
    def highWatermark: Long = underlying.hw
    def exception: Option[Throwable] =
      if (errorCode == ErrorMapping.NoError) None else Some(ErrorMapping.exceptionFor(errorCode))

  }
}
