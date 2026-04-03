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

class ReassignReplicaExpandTest extends TieredStorageTestHarness {

  private val (broker0, broker1, topicA, topicB, p0) = (0, 1, "topicA", "topicB", 0)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 2

  /* Create the '__remote_log_metadata' topic with 2 partitions */
  override protected def numMetadataPartitions: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val bTopicAssignment = Map(p0 -> Seq(broker0))
    builder
      // create topicA with 5 partitions and 2 RF
      .createTopic(topicA, partitionsCount = 5, replicationFactor = 2, maxBatchCountPerSegment = 1)
      .expectUserTopicMappedToMetadataPartitions(topicA, metadataPartitions = Seq(0 until numMetadataPartitions: _*))
      // create topicB with 1 partition and 1 RF
      .createTopic(topicB, partitionsCount = 1, replicationFactor = 1, maxBatchCountPerSegment = 1, bTopicAssignment)
      // send records to partition 0
      .produce(topicB, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectSegmentToBeOffloaded(broker0, topicB, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker0, topicB, p0, baseOffset = 1, ("k2", "v2"))
      .expectEarliestOffsetInLogDirectory(topicB, p0, earliestOffset = 2)
      // expand the topicB RF=2, the newly created replica gets mapped to one of the metadata partition which is being
      // actively consumed by both the brokers.
      .reassignReplica(topicB, p0, replicaIds = Seq(broker0, broker1))
      .expectLeader(topicB, p0, broker1, electLeader = true)
      // produce some more events
      .produce(topicB, p0, ("k4", "v4"))
      // verify the earliest offset in local log dir
      .expectEarliestOffsetInLogDirectory(topicB, p0, earliestOffset = 3)
      // consume from the beginning of the topic to read data from local and remote storage
      .expectFetchFromTieredStorage(broker1, topicB, p0, remoteFetchRequestCount = 3)
      .consume(topicB, p0, fetchOffset = 0, expectedTotalRecord = 4, expectedRecordsFromSecondTier = 3)
  }
}
