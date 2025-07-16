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
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.ReplicaState

import java.util.Optional

class MockTierStateMachine(leader: LeaderEndPoint) extends TierStateMachine(leader, null, false) {

  var fetcher: MockFetcherThread = _

  override def start(topicPartition: TopicPartition,
                     currentFetchState: PartitionFetchState,
                     fetchPartitionData: FetchResponseData.PartitionData): PartitionFetchState = {
    val leaderEndOffset = leader.fetchLatestOffset(topicPartition, currentFetchState.currentLeaderEpoch).offset
    val offsetToFetch = leader.fetchEarliestLocalOffset(topicPartition, currentFetchState.currentLeaderEpoch).offset
    val initialLag = leaderEndOffset - offsetToFetch
    fetcher.truncateFullyAndStartAt(topicPartition, offsetToFetch)
    new PartitionFetchState(currentFetchState.topicId, offsetToFetch, Optional.of(initialLag),
      currentFetchState.currentLeaderEpoch, Optional.empty(), ReplicaState.FETCHING,
      Optional.of(currentFetchState.currentLeaderEpoch)
    )
  }

  def setFetcher(mockFetcherThread: MockFetcherThread): Unit = {
    fetcher = mockFetcherThread
  }
}
