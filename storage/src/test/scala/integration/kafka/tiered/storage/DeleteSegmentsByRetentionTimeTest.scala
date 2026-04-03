/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package integration.kafka.tiered.storage

import kafka.tiered.storage.{TieredStorageTestBuilder, TieredStorageTestHarness}
import org.apache.kafka.common.config.TopicConfig

class DeleteSegmentsByRetentionTimeTest extends TieredStorageTestHarness {

  private val (broker0, topicA, p0) = (0, "topicA", 0)

  override protected def brokerCount: Int = 1

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    builder.createTopic(topicA, partitionsCount = 1, replicationFactor = 1, maxBatchCountPerSegment = 1)
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 1, ("k2", "v2"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 2, ("k3", "v3"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 3)
      .updateTopicConfig(topicA, Map(TopicConfig.RETENTION_MS_CONFIG -> 1.toString), Seq.empty)
      .expectDeletionInRemoteStorage(broker0, topicA, p0, atMostDeleteSegmentCallCount = 3)
      .waitForRemoteLogSegmentDeletion(topicA)
      .expectLeaderEpochCheckpoint(broker0, topicA, p0, beginEpoch = 0, startOffset = 3)
      .expectFetchFromTieredStorage(broker0, topicA, p0, remoteFetchRequestCount = 0)
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 1, expectedRecordsFromSecondTier = 0)
  }
}
