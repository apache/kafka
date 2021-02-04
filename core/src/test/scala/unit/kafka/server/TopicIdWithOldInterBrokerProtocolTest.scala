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

import java.util.{Arrays, Properties}

import kafka.api.KAFKA_2_7_IV0
import kafka.network.SocketServer
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteTopicsRequest, DeleteTopicsResponse, MetadataRequest, MetadataResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class TopicIdWithOldInterBrokerProtocolTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, KAFKA_2_7_IV0.toString)
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.DefaultReplicationFactorProp, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)
  }

  @Test
  def testMetadataTopicIdsWithOldIBP(): Unit = {
    val replicaAssignment = Map(0 -> Seq(1, 2, 0), 1 -> Seq(2, 0, 1))
    val topic1 = "topic1"
    createTopic(topic1, replicaAssignment)

    val resp = sendMetadataRequest(new MetadataRequest.Builder(Seq(topic1, topic1).asJava, true, 10, 10).build(), Some(notControllerSocketServer))
    assertEquals(1, resp.topicMetadata.size)
    resp.topicMetadata.forEach { topicMetadata =>
      assertEquals(Errors.NONE, topicMetadata.error)
      assertEquals(Uuid.ZERO_UUID, topicMetadata.topicId())
    }
  }

  @Test
  def testDeleteTopicsWithOldIBP(): Unit = {
    val timeout = 10000
    createTopic("topic-3", 5, 2)
    createTopic("topic-4", 1, 2)
    val request = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopicNames(Arrays.asList("topic-3", "topic-4"))
        .setTimeoutMs(timeout)).build()
    val resp = sendDeleteTopicsRequest(request)
    val error = resp.errorCounts.asScala.find(_._1 != Errors.NONE)
    assertTrue(error.isEmpty, s"There should be no errors, found ${resp.data.responses.asScala}")
    request.data.topicNames.forEach { topic =>
      validateTopicIsDeleted(topic)
    }
    resp.data.responses.forEach { response =>
      assertEquals(Uuid.ZERO_UUID, response.topicId())
    }
  }

  @Test
  def testDeleteTopicsWithOldIBPUsingIDs(): Unit = {
    val timeout = 10000
    createTopic("topic-7", 3, 2)
    createTopic("topic-6", 1, 2)
    val ids = Map("topic-7" -> Uuid.randomUuid(), "topic-6" -> Uuid.randomUuid())
    val request = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopics(Arrays.asList(new DeleteTopicState().setTopicId(ids("topic-7")),
          new DeleteTopicState().setTopicId(ids("topic-6"))
        )).setTimeoutMs(timeout)).build()
    val response = sendDeleteTopicsRequest(request)
    val error = response.errorCounts.asScala
    assertEquals(2, error(Errors.UNSUPPORTED_VERSION))
  }

  private def sendMetadataRequest(request: MetadataRequest, destination: Option[SocketServer]): MetadataResponse = {
    connectAndReceive[MetadataResponse](request, destination = destination.getOrElse(anySocketServer))
  }

  private def sendDeleteTopicsRequest(request: DeleteTopicsRequest, socketServer: SocketServer = controllerSocketServer): DeleteTopicsResponse = {
    connectAndReceive[DeleteTopicsResponse](request, destination = socketServer)
  }

  private def validateTopicIsDeleted(topic: String): Unit = {
    val metadata = connectAndReceive[MetadataResponse](new MetadataRequest.Builder(
      List(topic).asJava, true).build).topicMetadata.asScala
    TestUtils.waitUntilTrue (() => !metadata.exists(p => p.topic.equals(topic) && p.error == Errors.NONE),
      s"The topic $topic should not exist")
  }

}
