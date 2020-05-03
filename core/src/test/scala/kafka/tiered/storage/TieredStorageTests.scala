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

import java.util.Optional

import kafka.tiered.storage.TieredStorageTests.{CanFetchFromTieredStorageAfterRecoveryOfLocalSegmentsTest, OffloadAndConsumeFromLeaderTest}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.replica.{ClientMetadata, PartitionView, ReplicaSelector, ReplicaView}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Suite.SuiteClasses
import org.junit.runners.Suite

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

@SuiteClasses(Array[Class[_]](
  classOf[OffloadAndConsumeFromLeaderTest],
  classOf[CanFetchFromTieredStorageAfterRecoveryOfLocalSegmentsTest]
))
@RunWith(classOf[Suite])
object TieredStorageTests {

  /**
    * Test Cases (A):
    *
    *    Elementary offloads and fetches from tiered storage.
    */
  final class OffloadAndConsumeFromLeaderTest extends TieredStorageTestHarness {
    private val (broker, topicA, topicB, p0) = (0, "topicA", "topicB", 0)

    override protected def brokerCount: Int = 1

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
      builder
        /*
         * (A.1) Create a topic which segments contain only one record and produce three.
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
        .createTopic(topicA, partitionsCount = 1, replicationFactor = 1, segmentSize = 1)
        .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(broker, topicA, p0, baseOffset = 0, segmentSize = 1)
        .expectSegmentToBeOffloaded(broker, topicA, p0, baseOffset = 1, segmentSize = 1)

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
         *          | base offset = 2   |             |  base offset = 0  |
         *          | (k3, v3)          |             |  (k1, v1)         |
         *          *-------------------*             |  (k2, v2)         |
         *                                            *-------------------*
         */
        .createTopic(topicB, partitionsCount = 1, replicationFactor = 1, segmentSize = 2)
        .produce(topicB, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(broker, topicB, p0, baseOffset = 0, segmentSize = 2)

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
        .consume(topicB, p0, fetchOffset = 1, expectedTotalRecord = 2, expectedRecordsFromSecondTier = 1)
        .expectFetchFromTieredStorage(broker, topicA, p0, recordCount = 1)
        .expectFetchFromTieredStorage(broker, topicB, p0, recordCount = 1)
    }
  }

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
  final class CanFetchFromTieredStorageAfterRecoveryOfLocalSegmentsTest extends TieredStorageTestHarness {
    private val (broker0, broker1, topicA, p0) = (0, 1, "topicA", 0)

    /* Cluster of two brokers */
    override protected def brokerCount: Int = 2

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
      builder
        .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, segmentSize = 1)
        .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 0, segmentSize = 1)
        .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 1, segmentSize = 1)

        /*
         * (B.1) Stop B0 and read remote log segments from the leader replica which is expected
         *       to be moved to B1.
         */
        .expectLeader(topicA, p0, broker0)
        .stop(broker0)
        .expectLeader(topicA, p0, broker1)
        .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromSecondTier = 2)
        .expectFetchFromTieredStorage(broker1, topicA, p0, recordCount = 2)

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
        //      segments from the first-tier storage will be found in B0. We need to mechanism to deterministically
        //      initiate consumption once we know metadata are up-to-date in the restarted broker. Alternatively,
        //      we can evaluate if a stronger consistency is wished on broker restart such that a log
        //      is not available for consumption until the metadata for its remote segments have been processed.
        //
        .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromSecondTier = 2)
        .expectFetchFromTieredStorage(broker0, topicA, p0, recordCount = 2)
    }
  }

  final class UncleanLeaderElectionAndTieredStorageTest extends TieredStorageTestHarness {
    private val (leader, follower, _, topicA, p0) = (0, 1, 2, "topicA", 0)

    override protected def brokerCount: Int = 3

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
      val assignment = Map(p0 -> Seq(leader, follower))

      builder
        .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, segmentSize = 1, assignment)
        .produce(topicA, p0, ("k1", "v1"))

        .stop(follower)
        .produce(topicA, p0, ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(leader, topicA, p0, baseOffset = 1, segmentSize = 1)

        .stop(leader)
        .start(follower)
        .expectLeader(topicA, p0, follower)
        .produce(topicA, p0, ("k4", "v4"), ("k5", "v5"))
        .expectSegmentToBeOffloaded(follower, topicA, p0, baseOffset = 1, segmentSize = 1)
    }
  }

  //
  // TODO: fetch from follower not implemented.
  //
  final class OffloadAndConsumeFromFollowerTest extends TieredStorageTestHarness {

    override protected def brokerCount: Int = 2

    override protected def readReplicaSelectorClass: Option[Class[_ <: ReplicaSelector]] =
      Some(classOf[ConsumeFromFollowerInDualBrokerCluster])

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
      builder
        .createTopic("topicA", partitionsCount = 1, replicationFactor = 2, segmentSize = 1)
        .expectLeader("topicA", partition = 0, brokerId = 0)
        .expectInIsr("topicA", partition = 0, brokerId = 1)
        .produce("topicA", 0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(fromBroker = 0, "topicA", partition = 0, baseOffset = 0, segmentSize = 1)
        .expectSegmentToBeOffloaded(fromBroker = 0, "topicA", partition = 0, baseOffset = 1, segmentSize = 1)
        .consume("topicA", partition = 0, fetchOffset = 1, expectedTotalRecord = 2, expectedRecordsFromSecondTier = 1)

        .stop(brokerId = 1)
        .eraseBrokerStorage(brokerId = 1)
        .start(brokerId = 1)
        .expectLeader("topicA", partition = 0, brokerId = 0)
        .expectInIsr("topicA", partition = 0, brokerId = 1)
        .consume("topicA", partition = 0, fetchOffset = 0, expectedTotalRecord = 2, expectedRecordsFromSecondTier = 1)
        .expectFetchFromTieredStorage(fromBroker = 1, "topicA", partition = 0, recordCount = 2)
    }
  }
}

final class ConsumeFromFollowerInDualBrokerCluster extends ReplicaSelector {

  override def select(topicPartition: TopicPartition,
                      clientMetadata: ClientMetadata,
                      partitionView: PartitionView): Optional[ReplicaView] = {

    if (Topic.isInternal(topicPartition.topic())) {
      Some(partitionView.leader()).asJava

    } else {
      assertEquals(
        s"Replicas for the topic-partition $topicPartition need to be assigned to exactly two brokers.",
        2,
        partitionView.replicas().size()
      )

      partitionView.replicas().asScala.find(_ != partitionView.leader()).asJava
    }
  }
}

