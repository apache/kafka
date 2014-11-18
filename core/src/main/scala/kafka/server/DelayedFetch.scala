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

import kafka.network.RequestChannel
import kafka.api.{FetchResponse, FetchRequest}
import kafka.common.{UnknownTopicOrPartitionException, NotLeaderForPartitionException, TopicAndPartition}

import scala.collection.immutable.Map
import scala.collection.Seq

/**
 * A delayed fetch request, which is satisfied (or more
 * accurately, unblocked) -- if:
 * Case A: This broker is no longer the leader for some partitions it tries to fetch
 *   - should return whatever data is available for the rest partitions.
 * Case B: This broker is does not know of some partitions it tries to fetch
 *   - should return whatever data is available for the rest partitions.
 * Case C: The fetch offset locates not on the last segment of the log
 *   - should return all the data on that segment.
 * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
 *   - should return whatever data is available.
 */

class DelayedFetch(override val keys: Seq[TopicAndPartition],
                   override val request: RequestChannel.Request,
                   override val delayMs: Long,
                   val fetch: FetchRequest,
                   private val partitionFetchOffsets: Map[TopicAndPartition, LogOffsetMetadata])
  extends DelayedRequest(keys, request, delayMs) {

  def isSatisfied(replicaManager: ReplicaManager) : Boolean = {
    var accumulatedSize = 0
    val fromFollower = fetch.isFromFollower
    partitionFetchOffsets.foreach {
      case (topicAndPartition, fetchOffset) =>
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            val replica = replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
            val endOffset =
              if (fromFollower)
                replica.logEndOffset
              else
                replica.highWatermark

            if (endOffset.offsetOnOlderSegment(fetchOffset)) {
              // Case C, this can happen when the new follower replica fetching on a truncated leader
              debug("Satisfying fetch request %s since it is fetching later segments of partition %s.".format(fetch, topicAndPartition))
              return true
            } else if (fetchOffset.offsetOnOlderSegment(endOffset)) {
              // Case C, this can happen when the folloer replica is lagging too much
              debug("Satisfying fetch request %s immediately since it is fetching older segments.".format(fetch))
              return true
            } else if (fetchOffset.precedes(endOffset)) {
              accumulatedSize += endOffset.positionDiff(fetchOffset)
            }
          }
        } catch {
          case utpe: UnknownTopicOrPartitionException => // Case A
            debug("Broker no longer know of %s, satisfy %s immediately".format(topicAndPartition, fetch))
            return true
          case nle: NotLeaderForPartitionException =>  // Case B
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicAndPartition, fetch))
            return true
        }
    }

    // Case D
    accumulatedSize >= fetch.minBytes
  }

  def respond(replicaManager: ReplicaManager): FetchResponse = {
    val topicData = replicaManager.readMessageSets(fetch)
    FetchResponse(fetch.correlationId, topicData.mapValues(_.data))
  }
}