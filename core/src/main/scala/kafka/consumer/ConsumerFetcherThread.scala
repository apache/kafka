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

import kafka.cluster.Broker
import kafka.server.AbstractFetcherThread
import kafka.message.ByteBufferMessageSet
import kafka.api.{PartitionOffsetRequestInfo, Request, OffsetRequest, FetchResponsePartitionData}
import kafka.common.TopicAndPartition
import kafka.common.ErrorMapping


class ConsumerFetcherThread(name: String,
                            val config: ConsumerConfig,
                            sourceBroker: Broker,
                            partitionMap: Map[TopicAndPartition, PartitionTopicInfo],
                            val consumerFetcherManager: ConsumerFetcherManager)
        extends AbstractFetcherThread(name = name, 
                                      clientId = config.clientId + "-" + name,
                                      sourceBroker = sourceBroker,
                                      socketTimeout = config.socketTimeoutMs,
                                      socketBufferSize = config.socketReceiveBufferBytes,
                                      fetchSize = config.fetchMessageMaxBytes,
                                      fetcherBrokerId = Request.OrdinaryConsumerId,
                                      maxWait = config.fetchWaitMaxMs,
                                      minBytes = config.fetchMinBytes,
                                      isInterruptible = true) {

  // process fetched data
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: FetchResponsePartitionData) {
    val pti = partitionMap(topicAndPartition)
    if (pti.getFetchOffset != fetchOffset)
      throw new RuntimeException("Offset doesn't match for topic %s partition: %d pti offset: %d fetch offset: %d"
                                .format(topicAndPartition.topic, topicAndPartition.partition, pti.getFetchOffset, fetchOffset))
    pti.enqueue(partitionData.messages.asInstanceOf[ByteBufferMessageSet])
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    var startTimestamp : Long = 0
    config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => startTimestamp = OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => startTimestamp = OffsetRequest.LatestTime
      case _ => startTimestamp = OffsetRequest.LatestTime
    }
    val newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId)
    val pti = partitionMap(topicAndPartition)
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    consumerFetcherManager.addPartitionsWithError(partitions)
  }
}
