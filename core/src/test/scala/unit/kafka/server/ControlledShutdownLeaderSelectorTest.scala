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
    val liveBrokerIds = mutable.Set(1, 3, 4, 6)
    val shuttingDownBrokerIds = mutable.Set(2, 5)
    val assignment = Seq(6, 5, 4, 3, 2, 1)
    val topicPartition = TopicAndPartition("topic", 1)
    val isr = List(1, 3, 5)
    val partitionReplicaAssignment = mutable.Map(topicPartition -> assignment)

    val zkUtils = EasyMock.mock(classOf[ZkUtils])
    val controllerContext = new ControllerContext(zkUtils, zkSessionTimeout = 1000)
    controllerContext.liveBrokers = (liveBrokerIds ++ shuttingDownBrokerIds).map(Broker(_, Map.empty, None))
    controllerContext.shuttingDownBrokerIds = shuttingDownBrokerIds
    controllerContext.partitionReplicaAssignment = partitionReplicaAssignment

    val leaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
    val leaderAndIsr = new LeaderAndIsr(1, isr)
    val (newLeaderAndIsr, replicas) = leaderSelector.selectLeader(topicPartition, leaderAndIsr)

    assertEquals(3, newLeaderAndIsr.leader)
    assertEquals(Seq(1, 3), newLeaderAndIsr.isr)
    assertEquals(1, newLeaderAndIsr.zkVersion)
    assertEquals(1, newLeaderAndIsr.leaderEpoch)
  }

}
