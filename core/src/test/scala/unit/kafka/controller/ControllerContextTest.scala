/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import kafka.cluster.{Broker, EndPoint}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.Test

class ControllerContextTest {

  @Test
  def testLiveOrOfflineReplicas: Unit = {
    val endpoint1 = new EndPoint("localhost", 9997, new ListenerName("blah"),
      SecurityProtocol.PLAINTEXT)
    val endpoint2 = new EndPoint("localhost", 9998, new ListenerName("blah"),
      SecurityProtocol.PLAINTEXT)
    val endpoint3 = new EndPoint("localhost", 9999, new ListenerName("blah"),
      SecurityProtocol.PLAINTEXT)

    val liveBrokerEpochs = Map(
      Broker(1, Seq(endpoint1), rack = None) -> 1L,
      Broker(2, Seq(endpoint2), rack = None) -> 5L,
      Broker(3, Seq(endpoint3), rack = None) -> 2L
    )

    val context = new ControllerContext
    context.setLiveBrokerAndEpochs(liveBrokerEpochs)

    val partition0 = new TopicPartition("foo", 0)
    val partition1 = new TopicPartition("foo", 1)
    val partition2 = new TopicPartition("foo", 2)
    val partition3 = new TopicPartition("foo", 3)

    context.updatePartitionReplicaAssignment(partition0, Seq(1, 2, 3))
    context.updatePartitionReplicaAssignment(partition1, Seq(2, 3, 4))
    context.updatePartitionReplicaAssignment(partition2, Seq(3, 4, 5))
    context.updatePartitionReplicaAssignment(partition3, Seq(4, 5, 6))

    val expectedOfflineReplicas = Set(
      PartitionAndReplica(partition1, 4),
      PartitionAndReplica(partition2, 4),
      PartitionAndReplica(partition2, 5),
      PartitionAndReplica(partition3, 4),
      PartitionAndReplica(partition3, 5),
      PartitionAndReplica(partition3, 6)
    )

    val expectedOnlineReplicas = Set(
      PartitionAndReplica(partition0, 1),
      PartitionAndReplica(partition0, 2),
      PartitionAndReplica(partition0, 3),
      PartitionAndReplica(partition1, 2),
      PartitionAndReplica(partition1, 3),
      PartitionAndReplica(partition2, 3),
    )

    val (onlineReplicas, offlineReplicas) = context.liveOrOfflineReplicas
    assertEquals(expectedOnlineReplicas, onlineReplicas)
    assertEquals(expectedOfflineReplicas, offlineReplicas)
  }

}
