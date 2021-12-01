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

import scala.collection.Seq

class ReassignReplicaShrinkTest extends TieredStorageTestHarness {
  private val (broker0, broker1, topicA, p0, p1) = (0, 1, "topicA", 0, 1)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(broker0, broker1), p1 -> Seq(broker1, broker0))
    builder
      .createTopic(topicA, partitionsCount = 2, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment)
      // send records to partition 0, expect that the segments are uploaded and removed from local log dir
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 1, ("k2", "v2"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 2)
      // send records to partition 1, expect that the segments are uploaded and removed from local log dir
      .produce(topicA, p1, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectSegmentToBeOffloaded(broker1, topicA, p1, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker1, topicA, p1, baseOffset = 1, ("k2", "v2"))
      .expectEarliestOffsetInLogDirectory(topicA, p1, earliestOffset = 2)
      // shrink the replication factor to 1
      .shrinkReplica(topicA, p0, replicaIds = Seq(broker1))
      .shrinkReplica(topicA, p1, replicaIds = Seq(broker0))
      .expectLeader(topicA, p0, broker1)
      .expectLeader(topicA, p1, broker0)
      // produce some more events
      .produce(topicA, p0, ("k4", "v4"))
      .produce(topicA, p1, ("k4", "v4"))
      // verify the earliest offset in local log dir
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 3)
      .expectEarliestOffsetInLogDirectory(topicA, p1, earliestOffset = 3)
      // consume from the beginning of the topic to read data from local and remote storage
      .expectFetchFromTieredStorage(broker1, topicA, p0, remoteFetchRequestCount = 3)
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 4, expectedRecordsFromSecondTier = 3)
      .expectFetchFromTieredStorage(broker0, topicA, p1, remoteFetchRequestCount = 3)
      .consume(topicA, p1, fetchOffset = 0, expectedTotalRecord = 4, expectedRecordsFromSecondTier = 3)
  }
}
