/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.api.Request
import kafka.server.QuotaFactory.UnboundedQuota
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, RequestUtils}

import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class LocalLeaderEndPoint(override val brokerConfig: KafkaConfig,
                          val replicaMgr: ReplicaManager) extends LeaderEndPoint {

  override def fetch(fetchRequest: FetchRequest.Builder): collection.Map[TopicPartition, FetchData] = {
    var partitionData: Seq[(TopicPartition, FetchData)] = null
    val request = fetchRequest.build()

    // We can build the map from the request since it contains topic IDs and names.
    // Only one ID can be associated with a name and vice versa.
    val topicNames = new mutable.HashMap[Uuid, String]()
    request.data.topics.forEach { topic =>
      topicNames.put(topic.topicId, topic.topic)
    }

    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      partitionData = responsePartitionData.map { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        tp.topicPartition -> new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.topicPartition.partition)
          .setErrorCode(data.error.code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
      }
    }

    val fetchData = request.fetchData(topicNames.asJava)

    replicaMgr.fetchMessages(
      0L, // timeout is 0 so that the callback will be executed immediately
      Request.FutureLocalReplicaId,
      request.minBytes,
      request.maxBytes,
      false,
      fetchData.asScala.toSeq,
      UnboundedQuota,
      processResponseCallback,
      request.isolationLevel,
      None)

    if (partitionData == null)
      throw new IllegalStateException(s"Failed to fetch data for partitions ${fetchData.keySet().toArray.mkString(",")}")

    partitionData.toMap
  }

  override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logStartOffset
  }

  override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logEndOffset
  }

  /**
   * Fetches offset for leader epoch from local replica for each given topic partitions
   * @param partitions map of topic partition -> leader epoch of the future replica
   * @return map of topic partition -> end offset for a requested leader epoch
   */
  override def fetchEpochEndOffsets(partitions: collection.Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
    partitions.map { case (tp, epochData) =>
      try {
        val endOffset = if (epochData.leaderEpoch == UNDEFINED_EPOCH) {
          new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(Errors.NONE.code)
        } else {
          val partition = replicaMgr.getPartitionOrException(tp)
          partition.lastOffsetForLeaderEpoch(
            currentLeaderEpoch = RequestUtils.getLeaderEpoch(epochData.currentLeaderEpoch),
            leaderEpoch = epochData.leaderEpoch,
            fetchOnlyFromLeader = false)
        }
        tp -> endOffset
      } catch {
        case t: Throwable =>
          warn(s"Error when getting EpochEndOffset for $tp", t)
          tp -> new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(Errors.forException(t).code)
      }
    }
  }
}
