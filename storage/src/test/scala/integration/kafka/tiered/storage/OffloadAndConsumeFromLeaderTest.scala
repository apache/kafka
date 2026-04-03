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
 * Test Cases (A):
 *
 *    Elementary offloads and fetches from tiered storage.
 */
class OffloadAndConsumeFromLeaderTest extends TieredStorageTestHarness {
  private val (broker, topicA, topicB, p0) = (0, "topicA", "topicB", 0)

  override protected def brokerCount: Int = 1

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    builder

      /*
       * (A.1) Create a topic which segments contain only one batch and produce three records
       *       with a batch size of 1.
       *
       *       The topic and broker are configured so that the two rolled segments are picked from
       *       the offloaded to the tiered storage and not present in the first-tier broker storage.
       *
       *       Acceptance:
       *       -----------
       *       State of the storages after production of the records and propagation of the log
       *       segment lifecycles to peer subsystems (log cleaner, remote log manager).
       *
       *         - First-tier storage -            - Second-tier storage -
       *           Log tA-p0                         Log tA-p0
       *          *-------------------*             *-------------------*
       *          | base offset = 2   |             |  base offset = 0  |
       *          | (k3, v3)          |             |  (k1, v1)         |
       *          *-------------------*             *-------------------*
       *                                            *-------------------*
       *                                            |  base offset = 1  |
       *                                            |  (k2, v2)         |
       *                                            *-------------------*
       */
      .createTopic(topicA, partitionsCount = 1, replicationFactor = 1, maxBatchCountPerSegment = 1)
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .withBatchSize(topicA, p0, 1)
      .expectSegmentToBeOffloaded(broker, topicA, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker, topicA, p0, baseOffset = 1, ("k2", "v2"))

      /*
       * (A.2) Similar scenario as above, but with segments of two records.
       *
       *       Acceptance:
       *       -----------
       *       State of the storages after production of the records and propagation of the log
       *       segment lifecycles to peer subsystems (log cleaner, remote log manager).
       *
       *         - First-tier storage -            - Second-tier storage -
       *           Log tB-p0                         Log tB-p0
       *          *-------------------*             *-------------------*
       *          | base offset = 4   |             |  base offset = 0  |
       *          | (k5, v5)          |             |  (k1, v1)         |
       *          *-------------------*             |  (k2, v2)         |
       *                                            *-------------------*
       *                                            *-------------------*
       *                                            |  base offset = 2  |
       *                                            |  (k3, v3)         |
       *                                            |  (k4, v4)         |
       *                                            *-------------------*
       */
      .createTopic(topicB, partitionsCount = 1, replicationFactor = 1, maxBatchCountPerSegment = 2)
      .produce(topicB, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5"))
      .withBatchSize(topicB, p0, 1)
      .expectEarliestOffsetInLogDirectory(topicB, p0, 4)
      .expectSegmentToBeOffloaded(broker, topicB, p0, baseOffset = 0, ("k1", "v1"), ("k2", "v2"))
      .expectSegmentToBeOffloaded(broker, topicB, p0, baseOffset = 2, ("k3", "v3"), ("k4", "v4"))

      /*
       * (A.3) Stops and restarts the broker. The purpose of this test is to a) exercise consumption
       *       from a given offset and b) verify that upon broker start, existing remote log segments
       *       metadata are loaded by Kafka and these log segments available.
       *
       *       Acceptance:
       *       -----------
       *       - For topic A, this offset is defined such that only the second segment is fetched from
       *         the tiered storage.
       *       - For topic B, only one segment is present in the tiered storage, as asserted by the
       *         previous sub-test-case.
       */
      .bounce(broker)
      .consume(topicA, p0, fetchOffset = 1, expectedTotalRecord = 2, expectedRecordsFromSecondTier = 1)
      .consume(topicB, p0, fetchOffset = 1, expectedTotalRecord = 4, expectedRecordsFromSecondTier = 3)
      .expectFetchFromTieredStorage(broker, topicA, p0, remoteFetchRequestCount = 1)
      .expectFetchFromTieredStorage(broker, topicB, p0, remoteFetchRequestCount = 2)

      /*
       * (A.4) Scenario similar to (A.2) but with records produced as batches of three elements.
       *
       *       Note 1: the segment produced with a base offset 4 contains only 1 record, despite a
       *       max batch count per segment of 2. This is because when the broker is restarted, the
       *       timestamp of that record is appended to the time index, which only accepts two
       *       entries. When the batch A is appended to the log, the time index is detected as full
       *       (because the number of entries >= max-entries - 1), and the segment is rolled over.
       *
       *       Note 2: Records with key k1, k2, k3, k4 and k5 are also part of a batch - in that
       *       case, of size 1.
       *
       *       Acceptance:
       *       -----------
       *       - For topic B, 4 segments are present in the tiered storage. The fourth segments
       *         contains two batches of 3 records each. An additional batch is stored in the log
       *         directory in an active segment.
       *
       *          - First-tier storage -            - Second-tier storage -
       *           Log tB-p0                         Log tB-p0
       *          *-------------------*             *-------------------*
       *          | base offset = 11  |             |  base offset = 0  |
       *          |  ++++++++++++++   |             |  (k1, v1)         |
       *          |  + (k12, v12) +   |             |  (k2, v2)         |
       *          |  + (k13, v13) + C |             *-------------------*
       *          |  + (k14, v14) +   |             *-------------------*
       *          |  ++++++++++++++   |             |  base offset = 2  |
       *          *-------------------*             |  (k3, v3)         |
       *                                            |  (k4, v4)         |
       *                                            *-------------------*
       *                                            *-------------------*
       *                                            |  base offset = 4  |
       *                                            |  (k5, v5)         |
       *                                            *-------------------*
       *                                            *-------------------*
       *                                            |  base offset = 5  |
       *                                            |  ++++++++++++++   |
       *                                            |  +  (k6, v6)  +   |
       *                                            |  +  (k7, v7)  + A |
       *                                            |  +  (k8, v8)  +   |
       *                                            |  ++++++++++++++   |
       *                                            |  ++++++++++++++   |
       *                                            |  +  (k9, v9)  +   |
       *                                            |  + (k10, v10) + B |
       *                                            |  + (k11, v11) +   |
       *                                            |  ++++++++++++++   |
       *                                            *-------------------*
       */
      .produce(topicB, p0,
        ("k6", "v6"), ("k7", "v7"), ("k8", "v8"),        // First batch A
        ("k9", "v9"), ("k10", "v10"), ("k11", "v11"),    // Second batch B
        ("k12", "v12"), ("k13", "v13"), ("k14", "v14"))  // Third batch C
      .withBatchSize(topicB, p0, 3)
      .expectEarliestOffsetInLogDirectory(topicB, p0, 11)
      .expectSegmentToBeOffloaded(broker, topicB, p0, baseOffset = 4, ("k5", "v5"))
      .expectSegmentToBeOffloaded(
        broker, topicB, p0, baseOffset = 5,
        ("k6", "v6"), ("k7", "v7"), ("k8", "v8"), ("k9", "v9"), ("k10", "v10"), ("k11", "v11")
      )
      .consume(topicB, p0, fetchOffset = 0, expectedTotalRecord = 14, expectedRecordsFromSecondTier = 11)
      .expectFetchFromTieredStorage(broker, topicB, p0, remoteFetchRequestCount = 4)
  }
}
