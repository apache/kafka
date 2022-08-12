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

import kafka.network.SocketServer
import kafka.utils.TestUtils
import kafka.zk.ZkVersion
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.IBP_2_8_IV0
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.{Map, Seq}

class MetadataRequestBetweenDifferentIbpTest extends BaseRequestTest {

  override def brokerCount: Int = 3
  override def generateConfigs: Seq[KafkaConfig] = {
    Seq(
      createConfig(0, IBP_2_8_IV0),
      createConfig(1, MetadataVersion.latest),
      createConfig(2, MetadataVersion.latest)
    )
  }

  @Test
  def testUnknownTopicId(): Unit = {
    val topic = "topic"

    // Kill controller and restart until broker with latest ibp become controller
    ensureControllerIn(Seq(1, 2))
    createTopicWithAssignment(topic, Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1)))

    val resp1 = sendMetadataRequest(new MetadataRequest(requestData(topic, Uuid.ZERO_UUID), 12.toShort), controllerSocketServer)
    val topicId = resp1.topicMetadata.iterator().next().topicId()

    // We could still get topic metadata by topicId
   val topicMetadata = sendMetadataRequest(new MetadataRequest(requestData(null, topicId), 12.toShort), controllerSocketServer)
      .topicMetadata.iterator().next()
    assertEquals(topicId, topicMetadata.topicId())
    assertEquals(topic, topicMetadata.topic())

    // Make the broker whose version=IBP_2_8_IV0 controller
    ensureControllerIn(Seq(0))

    // Restart the broker whose ibp is higher, and the controller will send metadata request to it
    killBroker(1)
    restartDeadBrokers()

    // Send request to a broker whose ibp is higher and restarted just now
    val resp2 = sendMetadataRequest(new MetadataRequest(requestData(topic, topicId), 12.toShort), brokerSocketServer(1))
    assertEquals(Errors.UNKNOWN_TOPIC_ID, resp2.topicMetadata.iterator().next().error())
  }

  private def ensureControllerIn(brokerIds: Seq[Int]): Unit = {
    while (!brokerIds.contains(controllerSocketServer.config.brokerId)) {
      zkClient.deleteController(ZkVersion.MatchAnyVersion)
      TestUtils.waitUntilControllerElected(zkClient)
    }
  }

  private def createConfig(nodeId: Int, interBrokerVersion: MetadataVersion): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerVersion.version)
    KafkaConfig.fromProps(props)
  }

  def requestData(topic: String, topicId: Uuid): MetadataRequestData = {
    val data = new MetadataRequestData
    data.topics.add(new MetadataRequestData.MetadataRequestTopic().setName(topic).setTopicId(topicId))
    data
  }

  private def sendMetadataRequest(request: MetadataRequest, destination: SocketServer): MetadataResponse = {
    connectAndReceive[MetadataResponse](request, destination)
  }

}
