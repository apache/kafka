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

import org.apache.kafka.common.DirectoryId
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.server.partition.SimpleAssignmentState
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util

object AssignmentStateTest {
  import AbstractPartitionTest._

  def parameters: util.stream.Stream[Arguments] = util.List.of[Arguments](
    Arguments.of(
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array.emptyIntArray, Array.emptyIntArray, util.List.of[Int], Boolean.box(false)),
    Arguments.of(
      Array(brokerId, brokerId + 1),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array.emptyIntArray, Array.emptyIntArray, util.List.of[Int], Boolean.box(true)),
    Arguments.of(
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId + 3, brokerId + 4),
      Array(brokerId + 1),
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId + 3, brokerId + 4),
      Array.emptyIntArray,
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array.emptyIntArray,
      Array(brokerId + 1),
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      Array(brokerId + 1, brokerId + 2),
      Array(brokerId + 1, brokerId + 2),
      Array(brokerId),
      Array.emptyIntArray,
      util.List.of(brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      Array(brokerId + 2, brokerId + 3, brokerId + 4),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId + 3, brokerId + 4, brokerId + 5),
      Array.emptyIntArray,
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      Array(brokerId + 2, brokerId + 3, brokerId + 4),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId + 3, brokerId + 4, brokerId + 5),
      Array.emptyIntArray,
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      Array(brokerId + 2, brokerId + 3),
      Array(brokerId, brokerId + 1, brokerId + 2),
      Array(brokerId + 3, brokerId + 4, brokerId + 5),
      Array.emptyIntArray,
      util.List.of(brokerId, brokerId + 1, brokerId + 2), Boolean.box(true))
  ).stream()
}

class AssignmentStateTest extends AbstractPartitionTest {

  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def testPartitionAssignmentStatus(isr: Array[Int], replicas: Array[Int],
                                    adding: Array[Int], removing: Array[Int],
                                    original: util.List[Integer], isUnderReplicated: Boolean): Unit = {
    val partitionRegistration = new PartitionRegistration.Builder()
      .setLeader(brokerId)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(6)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .setAddingReplicas(adding)
      .setRemovingReplicas(removing)
      .build()

    // set the original replicas as the URP calculation will need them
    if (!original.isEmpty)
      partition.assignmentState = new SimpleAssignmentState(original)
    // do the test
    partition.makeLeader(partitionRegistration, isNew = false, offsetCheckpoints, None)
    val isReassigning = !adding.isEmpty || !removing.isEmpty
    assertEquals(isReassigning, partition.isReassigning)
    adding.foreach(r => assertTrue(partition.isAddingReplica(r)))
    if (adding.contains(brokerId))
      assertTrue(partition.isAddingLocalReplica)
    else
      assertFalse(partition.isAddingLocalReplica)

    assertEquals(isUnderReplicated, partition.isUnderReplicated)
  }
}
