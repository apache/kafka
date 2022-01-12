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

class EnableRemoteLogOnTopicInMiddleOfSegmentDeletionTest extends TieredStorageTestHarness {
  private val (broker0, broker1, topicA, p0) = (0, 1, "topicA", 0)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(broker0, broker1))
    builder
      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment,
        enableRemoteLogStorage = false)
      // send records to partition 0
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 0)
      // logically deletes the segment by incrementing the log-start-offset checkpoint
      .deleteRecords(topicA, p0, beforeOffset = 1)
      // enable remote log storage
      .updateTopicConfig(topicA, Map(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG -> "true"), Seq.empty)
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 1, ("k2", "v2"))
      .produce(topicA, p0, ("k4", "v4"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 2, ("k3", "v3"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 3)
      .expectFetchFromTieredStorage(broker0, topicA, p0, remoteFetchRequestCount = 2)
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromSecondTier = 2)
  }
}
