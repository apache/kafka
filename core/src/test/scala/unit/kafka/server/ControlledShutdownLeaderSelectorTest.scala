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
package kafka.server

import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.controller.{ControlledShutdownLeaderSelector, ControllerContext}
import org.easymock.EasyMock
import org.junit.{Assert, Test}
import Assert._
import kafka.cluster.Broker
import kafka.utils.ZkUtils

import scala.collection.mutable

class ControlledShutdownLeaderSelectorTest {

  @Test
  def testSelectLeader() {
    val topicPartition = TopicAndPartition("topic", 1)
    val assignment = Seq(6, 5, 4, 3, 2, 1)
    val preferredReplicaId = assignment.head

    val firstIsr = List(1, 3, 6)
    val firstLeader = 1

    val zkUtils = EasyMock.mock(classOf[ZkUtils])
    val controllerContext = new ControllerContext(zkUtils)
    controllerContext.liveBrokers = assignment.map(Broker(_, Seq.empty, None)).toSet
    controllerContext.shuttingDownBrokerIds = mutable.Set(2, 3)
    controllerContext.partitionReplicaAssignment = mutable.Map(topicPartition -> assignment)

    val leaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
    val firstLeaderAndIsr = new LeaderAndIsr(firstLeader, firstIsr)
    val (secondLeaderAndIsr, secondReplicas) = leaderSelector.selectLeader(topicPartition, firstLeaderAndIsr)

    assertEquals(preferredReplicaId, secondLeaderAndIsr.leader)
    assertEquals(Seq(1, 6), secondLeaderAndIsr.isr)
    assertEquals(1, secondLeaderAndIsr.zkVersion)
    assertEquals(1, secondLeaderAndIsr.leaderEpoch)
    assertEquals(assignment, secondReplicas)

    controllerContext.shuttingDownBrokerIds += preferredReplicaId

    val deadBrokerId = 2
    controllerContext.liveBrokers = controllerContext.liveOrShuttingDownBrokers.filter(_.id != deadBrokerId)
    controllerContext.shuttingDownBrokerIds -= deadBrokerId

    val (thirdLeaderAndIsr, thirdReplicas) = leaderSelector.selectLeader(topicPartition, secondLeaderAndIsr)

    assertEquals(1, thirdLeaderAndIsr.leader)
    assertEquals(Seq(1), thirdLeaderAndIsr.isr)
    assertEquals(2, thirdLeaderAndIsr.zkVersion)
    assertEquals(2, thirdLeaderAndIsr.leaderEpoch)
    assertEquals(Seq(6, 5, 4, 3, 1), thirdReplicas)

  }

}
