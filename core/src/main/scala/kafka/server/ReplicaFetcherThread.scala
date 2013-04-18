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

package kafka.server

import kafka.cluster.Broker
import kafka.message.ByteBufferMessageSet
import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest, FetchResponsePartitionData}
import kafka.common.{KafkaStorageException, TopicAndPartition, ErrorMapping}

class ReplicaFetcherThread(name:String,
                           sourceBroker: Broker,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                socketTimeout = brokerConfig.replicaSocketTimeoutMs,
                                socketBufferSize = brokerConfig.replicaSocketReceiveBufferBytes,
                                fetchSize = brokerConfig.replicaFetchMaxBytes,
                                fetcherBrokerId = brokerConfig.brokerId,
                                maxWait = brokerConfig.replicaFetchWaitMaxMs,
                                minBytes = brokerConfig.replicaFetchMinBytes,
                                isInterruptible = false) {

  // process fetched data
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: FetchResponsePartitionData) {
    try {
      val topic = topicAndPartition.topic
      val partitionId = topicAndPartition.partition
      val replica = replicaMgr.getReplica(topic, partitionId).get
      val messageSet = partitionData.messages.asInstanceOf[ByteBufferMessageSet]

      if (fetchOffset != replica.logEndOffset)
        throw new RuntimeException("Offset mismatch: fetched offset = %d, log end offset = %d.".format(fetchOffset, replica.logEndOffset))
      trace("Follower %d has replica log end offset %d. Received %d messages and leader hw %d"
            .format(replica.brokerId, replica.logEndOffset, messageSet.sizeInBytes, partitionData.hw))
      replica.log.get.append(messageSet, assignOffsets = false)
      trace("Follower %d has replica log end offset %d after appending %d bytes of messages"
            .format(replica.brokerId, replica.logEndOffset, messageSet.sizeInBytes))
      val followerHighWatermark = replica.logEndOffset.min(partitionData.hw)
      replica.highWatermark = followerHighWatermark
      trace("Follower %d set replica highwatermark for topic %s partition %d to %d"
            .format(replica.brokerId, topic, partitionId, followerHighWatermark))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error while replicating data.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    val replica = replicaMgr.getReplica(topicAndPartition.topic, topicAndPartition.partition).get
    val log = replica.log.get

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val leaderEndOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.LatestTime, brokerConfig.brokerId)
    if (leaderEndOffset < log.logEndOffset) {
      log.truncateTo(leaderEndOffset)
      leaderEndOffset
    } else {
      /**
       * The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       *
       * Roll out a new log at the follower with the start offset equal to the current leader's start offset and continue fetching.
       */
      val leaderStartOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.EarliestTime, brokerConfig.brokerId)
      log.truncateFullyAndStartAt(leaderStartOffset)
      leaderStartOffset
    }
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    // no handler needed since the controller will make the changes accordingly
  }
}
