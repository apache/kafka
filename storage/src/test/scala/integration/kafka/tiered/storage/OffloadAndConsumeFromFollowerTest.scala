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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.replica.{ClientMetadata, PartitionView, ReplicaSelector, ReplicaView}

import java.util.Optional

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

/**
 * Test Case (D):
 *
 *   Fetch from a follower backed by a fetch to the tier storage.
 */
class OffloadAndConsumeFromFollowerTest extends TieredStorageTestHarness {
  private val (leader, follower, topicA, p0) = (0, 1, "topicA", 0)

  override protected def brokerCount: Int = 2

  override protected def readReplicaSelectorClass: Option[Class[_ <: ReplicaSelector]] =
    Some(classOf[ConsumeFromFollowerInDualBrokerCluster])

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    builder

      /**
       * Two segments with one single-record batch each are offloaded by the leader.
       * Records are consumed from the offset 1 from the follower.
       *
       * Acceptance:
       * -----------
       * Two records are read and one segment with base offset 1 is retrieved from the tier storage.
       */
      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1)
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectSegmentToBeOffloaded(leader, topicA, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(leader, topicA, p0, baseOffset = 1, ("k2", "v2"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, 2)
      .consume(topicA, p0, fetchOffset = 1, expectedTotalRecord = 2, expectedRecordsFromSecondTier = 1)
      .expectFetchFromTieredStorage(follower, topicA, 0, 1)
  }
}

final class ConsumeFromFollowerInDualBrokerCluster extends ReplicaSelector {

  override def select(topicPartition: TopicPartition,
                      clientMetadata: ClientMetadata,
                      partitionView: PartitionView): Optional[ReplicaView] = {

    if (Topic.isInternal(topicPartition.topic())) {
      Some(partitionView.leader()).asJava
    } else {
      partitionView.replicas().asScala.find(_ != partitionView.leader()).asJava
    }
  }
}
