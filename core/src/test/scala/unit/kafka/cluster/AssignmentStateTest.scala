/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.cluster

import org.apache.kafka.common.PartitionState
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util

import scala.jdk.CollectionConverters._

object AssignmentStateTest {
  import AbstractPartitionTest._

  def parameters: util.stream.Stream[Arguments] = util.List.of[Arguments](
    Arguments.of(
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer], util.List.of[Integer], util.List.of[Int], Boolean.box(false)),
    Arguments.of(
      util.List.of[Integer](brokerId, brokerId + 1),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer], util.List.of[Integer], util.List.of[Int], Boolean.box(true)),
    Arguments.of(
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId + 3, brokerId + 4),
      util.List.of[Integer](brokerId + 1),
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId + 3, brokerId + 4),
      util.List.of[Integer],
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer],
      util.List.of[Integer](brokerId + 1),
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      util.List.of[Integer](brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId),
      util.List.of[Integer],
      util.List.of(brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      util.List.of[Integer](brokerId + 2, brokerId + 3, brokerId + 4),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId + 3, brokerId + 4, brokerId + 5),
      util.List.of[Integer],
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      util.List.of[Integer](brokerId + 2, brokerId + 3, brokerId + 4),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId + 3, brokerId + 4, brokerId + 5),
      util.List.of[Integer],
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      util.List.of[Integer](brokerId + 2, brokerId + 3),
      util.List.of[Integer](brokerId, brokerId + 1, brokerId + 2),
      util.List.of[Integer](brokerId + 3, brokerId + 4, brokerId + 5),
      util.List.of[Integer],
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(true))
  ).stream()
}

class AssignmentStateTest extends AbstractPartitionTest {

  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def testPartitionAssignmentStatus(isr: util.List[Integer], replicas: util.List[Integer],
                                    adding: util.List[Integer], removing: util.List[Integer],
                                    original: util.List[Int], isUnderReplicated: Boolean): Unit = {
    val leaderState = new PartitionState()
      .setLeader(brokerId)
      .setLeaderEpoch(6)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setIsNew(false)
    if (!adding.isEmpty)
      leaderState.setAddingReplicas(adding)
    if (!removing.isEmpty)
      leaderState.setRemovingReplicas(removing)

    val isReassigning = !adding.isEmpty || !removing.isEmpty

    // set the original replicas as the URP calculation will need them
    if (!original.isEmpty)
      partition.assignmentState = SimpleAssignmentState(original.asScala)
    // do the test
    partition.makeLeader(leaderState, offsetCheckpoints, None)
    assertEquals(isReassigning, partition.isReassigning)
    if (!adding.isEmpty)
      adding.forEach(r => assertTrue(partition.isAddingReplica(r)))
    if (adding.contains(brokerId))
      assertTrue(partition.isAddingLocalReplica)
    else
      assertFalse(partition.isAddingLocalReplica)

    assertEquals(isUnderReplicated, partition.isUnderReplicated)
  }
}
