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

package unit.kafka.server

import kafka.server.{Fetching, LeaderEndPoint, MockTierStateMachine, PartitionFetchState, ReplicaManager}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.FetchResponseData

class MockTierStateMachineWithRlm(leader: LeaderEndPoint, replicaMgr: ReplicaManager) extends MockTierStateMachine(leader, replicaMgr) {

  override def start(topicPartition: TopicPartition,
                     currentFetchState: PartitionFetchState,
                     fetchPartitionData: FetchResponseData.PartitionData): PartitionFetchState = {
    val epochAndLeaderLocalStartOffset = leader.fetchEarliestLocalOffset(topicPartition, currentFetchState.currentLeaderEpoch)
    val epoch = epochAndLeaderLocalStartOffset.leaderEpoch
    val leaderLocalStartOffset = epochAndLeaderLocalStartOffset.offset

    val offsetToFetch = buildRemoteLogAuxState(topicPartition, currentFetchState.currentLeaderEpoch, leaderLocalStartOffset, epoch, fetchPartitionData.logStartOffset)

    val fetchLatestOffsetResult = leader.fetchLatestOffset(topicPartition, currentFetchState.currentLeaderEpoch)
    val leaderEndOffset = fetchLatestOffsetResult.offset

    val initialLag = leaderEndOffset - offsetToFetch

    PartitionFetchState.apply(currentFetchState.topicId, offsetToFetch, Option.apply(initialLag), currentFetchState.currentLeaderEpoch, Fetching, replicaMgr.localLogOrException(topicPartition).latestEpoch)
  }
}
