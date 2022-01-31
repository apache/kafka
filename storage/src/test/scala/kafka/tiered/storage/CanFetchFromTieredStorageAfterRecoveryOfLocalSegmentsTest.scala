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

package kafka.tiered.storage

/**
 * Test Cases (B):
 *
 *    Given a cluster of brokers {B0, B1} and a topic-partition Ta-p0.
 *    The purpose of this test is to exercise multiple failure scenarios on the cluster upon
 *    on a single-broker outage and loss of the first-tiered storage on one broker, that is:
 *
 *    - Loss of the remote log segment metadata on B0;
 *    - Loss of the active log segment on B0;
 *    - Loss of availability of broker B0;
 *
 *    Acceptance:
 *    -----------
 *    - Remote log segments of Ta-p0 are fetched from B1 when B0 is offline.
 *    - B0 restores the availability both active and remote log segments upon restart.
 *    - This assumes the remote log metadata which were lost on B0 can be recovered.
 *      To that end, the remote log metadata topic should have a replication factor of at least two
 *      so that the lost partitions on B0 can be recovered from B1.
 */
//
// TODO In order to assess the last point deterministically, move all leaders of the metadata
//      topic-partitions to B0 with replication on B1.
//
class CanFetchFromTieredStorageAfterRecoveryOfLocalSegmentsTest extends TieredStorageTestHarness {
  private val (broker0, broker1, topicA, p0) = (0, 1, "topicA", 0)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    builder

      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1)
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .withBatchSize(topicA, p0, 1)
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 1, ("k2", "v2"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, 2)

      /*
       * (B.1) Stop B0 and read remote log segments from the leader replica which is expected
       *       to be moved to B1.
       */
      .expectLeader(topicA, p0, broker0)
      .stop(broker0)
      .expectLeader(topicA, p0, broker1)
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromSecondTier = 2)
      .expectFetchFromTieredStorage(broker1, topicA, p0, remoteFetchRequestCount = 2)

      /*
       * (B.2) Restore previous leader with an empty storage. The active segment is expected to be
       *       replicated from the new leader.
       *       Note that a preferred leader election is manually triggered for broker 0 to avoid
       *       waiting on the election which would be automatically triggered.
       */
      .eraseBrokerStorage(broker0)
      .start(broker0)
      .expectLeader(topicA, p0, broker0, electLeader = true)
      //
      // TODO There is a race condition here. If consumption happens "too soon" and the remote log metadata
      //      manager has not been given enough time to update the remote log metadata, only the
      //      segments from the local storage will be found in B0. We need to mechanism to deterministically
      //      initiate consumption once we know metadata are up-to-date in the restarted broker. Alternatively,
      //      we can evaluate if a stronger consistency is wished on broker restart such that a log
      //      is not available for consumption until the metadata for its remote segments have been processed.
      //
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromSecondTier = 2)
      .expectFetchFromTieredStorage(broker0, topicA, p0, remoteFetchRequestCount = 2)
  }
}
