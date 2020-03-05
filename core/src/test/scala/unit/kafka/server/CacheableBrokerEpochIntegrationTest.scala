/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import java.util.Properties

import kafka.api.KAFKA_2_3_IV1
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.junit.Test

class CacheableBrokerEpochIntegrationTest extends ZooKeeperTestHarness {
  @Test
  def testNewControllerConfig(): Unit = {
    testControlRequests(true)
  }

  @Test
  def testOldControllerConfig(): Unit = {
    testControlRequests(false)
  }

  def testControlRequests(controllerUseNewConfig: Boolean): Unit = {
    val controllerId = 0
    val controllerConfig: Properties =
      if (controllerUseNewConfig) {
        TestUtils.createBrokerConfig(controllerId, zkConnect)
      } else {
        val oldConfig = TestUtils.createBrokerConfig(controllerId, zkConnect)
        oldConfig.put(KafkaConfig.InterBrokerProtocolVersionProp, KAFKA_2_3_IV1.toString)
        oldConfig
      }
    val controller = TestUtils.createServer(KafkaConfig.fromProps(controllerConfig))

    // Note that broker side logic does not depend on the InterBrokerProtocolVersion config
    val brokerId = 1
    val brokerConfig = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val broker = TestUtils.createServer(KafkaConfig.fromProps(brokerConfig))
    val servers = Seq(controller, broker)

    val tp = new TopicPartition("new-topic", 0)

    try {
      // Use topic creation to test the LeaderAndIsr and UpdateMetadata requests
      TestUtils.createTopic(zkClient, tp.topic(), partitionReplicaAssignment = Map(0 -> Seq(brokerId, controllerId)),
        servers = servers)
      TestUtils.waitUntilLeaderIsKnown(Seq(broker), tp, 10000)
      TestUtils.waitUntilMetadataIsPropagated(Seq(broker), tp.topic(), tp.partition())

      // Use topic deletion to test StopReplica requests
      adminZkClient.deleteTopic(tp.topic())
      TestUtils.verifyTopicDeletion(zkClient, tp.topic(), 1, servers)
    } finally {
      TestUtils.shutdownServers(servers)
    }
  }
}
