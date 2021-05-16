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

import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.jdk.CollectionConverters._

object AssignmentStateTest {
  import AbstractPartitionTest._

  def parameters: java.util.stream.Stream[Arguments] = Seq[Arguments](
    Arguments.of(
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List.empty[Integer], List.empty[Integer], Seq.empty[Int], Boolean.box(false)),
    Arguments.of(
      List[Integer](brokerId, brokerId + 1),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List.empty[Integer], List.empty[Integer], Seq.empty[Int], Boolean.box(true)),
    Arguments.of(
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId + 3, brokerId + 4),
      List[Integer](brokerId + 1),
      Seq(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId + 3, brokerId + 4),
      List.empty[Integer],
      Seq(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List.empty[Integer],
      List[Integer](brokerId + 1),
      Seq(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      List[Integer](brokerId + 1, brokerId + 2),
      List[Integer](brokerId + 1, brokerId + 2),
      List[Integer](brokerId),
      List.empty[Integer],
      Seq(brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      List[Integer](brokerId + 2, brokerId + 3, brokerId + 4),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId + 3, brokerId + 4, brokerId + 5),
      List.empty[Integer],
      Seq(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      List[Integer](brokerId + 2, brokerId + 3, brokerId + 4),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId + 3, brokerId + 4, brokerId + 5),
      List.empty[Integer],
      Seq(brokerId, brokerId + 1, brokerId + 2), Boolean.box(false)),
    Arguments.of(
      List[Integer](brokerId + 2, brokerId + 3),
      List[Integer](brokerId, brokerId + 1, brokerId + 2),
      List[Integer](brokerId + 3, brokerId + 4, brokerId + 5),
      List.empty[Integer],
      Seq(brokerId, brokerId + 1, brokerId + 2), Boolean.box(true))
  ).asJava.stream()
}

class AssignmentStateTest extends AbstractPartitionTest {

  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def testPartitionAssignmentStatus(isr: List[Integer], replicas: List[Integer],
                                    adding: List[Integer], removing: List[Integer],
                                    original: Seq[Int], isUnderReplicated: Boolean): Unit = {
    val controllerEpoch = 3

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(6)
      .setIsr(isr.asJava)
      .setZkVersion(1)
      .setReplicas(replicas.asJava)
      .setIsNew(false)
    if (adding.nonEmpty)
      leaderState.setAddingReplicas(adding.asJava)
    if (removing.nonEmpty)
      leaderState.setRemovingReplicas(removing.asJava)

    val isReassigning = adding.nonEmpty || removing.nonEmpty

    // set the original replicas as the URP calculation will need them
    if (original.nonEmpty)
      partition.assignmentState = SimpleAssignmentState(original)
    // do the test
    partition.makeLeader(leaderState, offsetCheckpoints, None)
    assertEquals(isReassigning, partition.isReassigning)
    if (adding.nonEmpty)
      adding.foreach(r => assertTrue(partition.isAddingReplica(r)))
    if (adding.contains(brokerId))
      assertTrue(partition.isAddingLocalReplica)
    else
      assertFalse(partition.isAddingLocalReplica)

    assertEquals(isUnderReplicated, partition.isUnderReplicated)
  }
}
