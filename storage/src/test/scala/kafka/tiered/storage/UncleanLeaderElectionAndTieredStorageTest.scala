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

class UncleanLeaderElectionAndTieredStorageTest extends TieredStorageTestHarness {
  private val (leader, follower, _, topicA, p0) = (0, 1, 2, "topicA", 0)

  override protected def brokerCount: Int = 3

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(leader, follower))

    builder
      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment)
      .produce(topicA, p0, ("k1", "v1"))

      .stop(follower)
      .produce(topicA, p0, ("k2", "v2"), ("k3", "v3"))
      .withBatchSize(topicA, p0, 1)
      .expectSegmentToBeOffloaded(leader, topicA, p0, baseOffset = 0, ("k1", "v1"))

      .stop(leader)
      .start(follower)
      .expectLeader(topicA, p0, follower)
      .produce(topicA, p0, ("k4", "v4"), ("k5", "v5"))
      .withBatchSize(topicA, p0, 1)
      // .expectSegmentToBeOffloaded(follower, topicA, p0, baseOffset = 1, ("k2", "v2"))
  }
}
