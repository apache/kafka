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
import kafka.api.{PartitionOffsetRequestInfo, Request, OffsetRequest, PartitionData}
import kafka.common.TopicAndPartition


class ConsumerFetcherThread(name: String,
                            val config: ConsumerConfig,
                            sourceBroker: Broker,
                            val consumerFetcherManager: ConsumerFetcherManager)
        extends AbstractFetcherThread(name = name, sourceBroker = sourceBroker, socketTimeout = config.socketTimeoutMs,
          socketBufferSize = config.socketBufferSize, fetchSize = config.fetchSize,
          fetcherBrokerId = Request.NonFollowerId, maxWait = config.maxFetchWaitMs,
          minBytes = config.minFetchBytes) {

  // process fetched data
  def processPartitionData(topic: String, fetchOffset: Long, partitionData: PartitionData) {
    val pti = consumerFetcherManager.getPartitionTopicInfo((topic, partitionData.partition))
    if (pti.getFetchOffset != fetchOffset)
      throw new RuntimeException("offset doesn't match for topic %s partition: %d pti offset: %d fetch ofset: %d"
                                .format(topic, partitionData.partition, pti.getFetchOffset, fetchOffset))
    pti.enqueue(partitionData.messages.asInstanceOf[ByteBufferMessageSet])
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topic: String, partitionId: Int): Long = {
    var startTimestamp : Long = 0
    config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => startTimestamp = OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => startTimestamp = OffsetRequest.LatestTime
      case _ => startTimestamp = OffsetRequest.LatestTime
    }
    val topicAndPartition = TopicAndPartition(topic, partitionId)
    val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(startTimestamp, 1)))
    val newOffset = simpleConsumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
    val pti = consumerFetcherManager.getPartitionTopicInfo((topic, partitionId))
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    consumerFetcherManager.addPartitionsWithError(partitions)
  }
}
