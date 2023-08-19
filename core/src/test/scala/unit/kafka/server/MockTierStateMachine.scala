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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FetchResponseData

import java.util.Optional

class MockTierStateMachine(leader: LeaderEndPoint) extends ReplicaFetcherTierStateMachine(leader, null) {

  var fetcher: MockFetcherThread = null

  override def start(topicPartition: TopicPartition,
                     currentFetchState: PartitionFetchState,
                     fetchPartitionData: FetchResponseData.PartitionData): PartitionFetchState = {
    val leaderEndOffset = leader.fetchLatestOffset(topicPartition, currentFetchState.currentLeaderEpoch).offset
    val offsetToFetch = leader.fetchEarliestLocalOffset(topicPartition, currentFetchState.currentLeaderEpoch).offset
    val initialLag = leaderEndOffset - offsetToFetch
    fetcher.truncateFullyAndStartAt(topicPartition, offsetToFetch)
    PartitionFetchState(currentFetchState.topicId, offsetToFetch, Option.apply(initialLag), currentFetchState.currentLeaderEpoch,
      Fetching, Some(currentFetchState.currentLeaderEpoch))
  }

  override def maybeAdvanceState(topicPartition: TopicPartition,
                                 currentFetchState: PartitionFetchState): Optional[PartitionFetchState] = {
    Optional.of(currentFetchState)
  }

  def setFetcher(mockFetcherThread: MockFetcherThread): Unit = {
    fetcher = mockFetcherThread
  }
}
