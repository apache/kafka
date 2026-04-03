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

package unit.kafka.integration

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, MoveControllerOptions}
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.{Node, TopicPartition}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Properties}

class AlterIsrRequestTest extends ZooKeeperTestHarness {
  @Test
  def testUnauthorizedAlterISRRequest(): Unit = {
    // create brokers
    val totalBrokers = 4
    val serverConfigs = TestUtils.createBrokerConfigs(totalBrokers, zkConnect, false)
      .map(props => {
        if (props.get(KafkaConfig.BrokerIdProp).equals((totalBrokers - 1).toString)) {
          props.setProperty(KafkaConfig.LiDenyAlterIsrProp, "true")
        }
        if (props.get(KafkaConfig.BrokerIdProp).equals("0")) {
          // let the leader drop Fetch requests from the followers, which will cause the partition to shrink its ISR
          props.setProperty(KafkaConfig.LiDropFetchFollowerEnableProp, "true")
        }
        props.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp, "10000")
        props
      })
      .map(KafkaConfig.fromProps)
    // start servers in reverse order to ensure the last broker becomes the controller
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val firstControllerId = TestUtils.waitUntilControllerElected(zkClient)
    val firstControllerEpoch = zkClient.getControllerEpoch.get._1
    assertTrue(firstControllerId == totalBrokers - 1)
    info(s"First elected controller is $firstControllerId")

    val topic = "test"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val topicConfig = new Properties()
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)

    // ensure the ISR has a size of 3
    val adminClient = TestUtils.createAdminClient(servers)
    val initialISR = getISR(adminClient, tp)
    assertEquals(3, initialISR.size())
    info(s"The initial ISR size is $initialISR")

    /**
     * Produce 1 message to trigger a mismatch of log end offset between the leader and followers.
     * This should further trigger an AlterISRRequest from the leader to the controller
     */
    val producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers), acks=1)
    produceRecord(producer, topic, partition)

    waitForDifferentController(adminClient, firstControllerId, firstControllerEpoch)

    // Ensure that the AlterISR request can go through with the new controller
    TestUtils.waitUntilTrue(() => {
      val currentISR = getISR(adminClient, tp)
      info(s"current isr $currentISR")
      currentISR.size() == 1
    }, "Unable to update the ISR despite a new controller", pause = 2000)

    info("Test has finished, shutting down the clients and servers")
    producer.close()
    adminClient.close()
    servers.foreach(_.shutdown())
  }

  private def getISR(adminClient: Admin, tp: TopicPartition): util.List[Node] = {
    val topicDescMap = adminClient.describeTopics(Collections.singleton(tp.topic())).all().get()
    topicDescMap.get(tp.topic()).partitions().get(tp.partition()).isr()
  }

  private def produceRecord(producer: Producer[Array[Byte], Array[Byte]], topic: String, partition: Int) = {
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
      "value".getBytes(StandardCharsets.UTF_8))
    producer.send(record).get()
  }

  def waitForDifferentController(adminClient: Admin, firstControllerId: Int, firstControllerEpoch: Int) = {
    var latestController = firstControllerId
    while (latestController == firstControllerId) {
      adminClient.moveController(new MoveControllerOptions())

      TestUtils.waitUntilTrue(() => {
        zkClient.getControllerEpoch.get._1 != firstControllerEpoch
      }, "The controller epoch does not change after a controller move")

      latestController = zkClient.getControllerId.get
    }
    info(s"Elected new controller $latestController")
  }
}
