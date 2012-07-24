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

import kafka.api.{OffsetRequest, PartitionData}
import kafka.cluster.Broker
import kafka.message.ByteBufferMessageSet

class ReplicaFetcherThread(name:String, sourceBroker: Broker, brokerConfig: KafkaConfig, replicaMgr: ReplicaManager)
  extends AbstractFetcherThread(name = name, sourceBroker = sourceBroker, socketTimeout = brokerConfig.replicaSocketTimeoutMs,
    socketBufferSize = brokerConfig.replicaSocketBufferSize, fetchSize = brokerConfig.replicaFetchSize,
    fetcherBrokerId = brokerConfig.brokerId, maxWait = brokerConfig.replicaMaxWaitTimeMs,
    minBytes = brokerConfig.replicaMinBytes) {

  // process fetched data
  def processPartitionData(topic: String, fetchOffset: Long, partitionData: PartitionData) {
    val partitionId = partitionData.partition
    val replica = replicaMgr.getReplica(topic, partitionId).get
    val messageSet = partitionData.messages.asInstanceOf[ByteBufferMessageSet]

    if (fetchOffset != replica.logEndOffset())
      throw new RuntimeException("offset mismatch: fetchOffset=%d, logEndOffset=%d".format(fetchOffset, replica.logEndOffset()))
    trace("Follower %d has replica log end offset %d. Received %d messages and leader hw %d".format(replica.brokerId,
      replica.logEndOffset(), messageSet.sizeInBytes, partitionData.hw))
    replica.log.get.append(messageSet)
    trace("Follower %d has replica log end offset %d after appending %d messages"
      .format(replica.brokerId, replica.logEndOffset(), messageSet.sizeInBytes))
    val followerHighWatermark = replica.logEndOffset().min(partitionData.hw)
    replica.highWatermark(Some(followerHighWatermark))
    trace("Follower %d set replica highwatermark for topic %s partition %d to %d"
      .format(replica.brokerId, topic, partitionId, followerHighWatermark))
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topic: String, partitionId: Int): Long = {
    // This means the local replica is out of date. Truncate the log and catch up from beginning.
    val offsets = simpleConsumer.getOffsetsBefore(topic, partitionId, OffsetRequest.EarliestTime, 1)
    val replica = replicaMgr.getReplica(topic, partitionId).get
    replica.log.get.truncateAndStartWithNewOffset(offsets(0))
    return offsets(0)
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[(String, Int)]) {
    // no handler needed since the controller will make the changes accordingly
  }
}
