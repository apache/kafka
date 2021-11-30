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

class EnableRemoteLogOnTopicTest extends TieredStorageTestHarness {

  private val (broker0, broker1, topicA, p0, p1) = (0, 1, "topicA", 0, 1)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(broker0, broker1), p1 -> Seq(broker1, broker0))
    builder
      .createTopic(topicA, partitionsCount = 2, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment,
        enableRemoteLogStorage = false)
      // send records to partition 0
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 0)
      // send records to partition 1
      .produce(topicA, p1, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectEarliestOffsetInLogDirectory(topicA, p1, earliestOffset = 0)
      // enable remote log storage
      .updateTopicConfig(topicA, Map(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG -> "true"), Seq.empty)
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 1, ("k2", "v2"))
      .expectSegmentToBeOffloaded(broker1, topicA, p1, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker1, topicA, p1, baseOffset = 1, ("k2", "v2"))
      .produce(topicA, p0, ("k4", "v4"))
      .produce(topicA, p1, ("k4", "v4"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 2, ("k3", "v3"))
      .expectSegmentToBeOffloaded(broker1, topicA, p1, baseOffset = 2, ("k3", "v3"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 3)
      .expectEarliestOffsetInLogDirectory(topicA, p1, earliestOffset = 3)
      .expectFetchFromTieredStorage(broker0, topicA, p0, remoteFetchRequestCount = 3)
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 4, expectedRecordsFromSecondTier = 3)
      .expectFetchFromTieredStorage(broker1, topicA, p1, remoteFetchRequestCount = 3)
      .consume(topicA, p1, fetchOffset = 0, expectedTotalRecord = 4, expectedRecordsFromSecondTier = 3)
  }
}
