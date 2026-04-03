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

import java.util.Collections

import kafka.utils._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteTopicsRequest, DeleteTopicsResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

class DeleteTopicsRequestWithDeletionDisabledTest extends BaseRequestTest {

  override def brokerCount: Int = 1

  override def generateConfigs = {
    val props = TestUtils.createBrokerConfigs(brokerCount, zkConnect,
      enableControlledShutdown = false, enableDeleteTopic = false,
      interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, logDirCount = logDirCount)
    props.foreach(brokerPropertyOverrides)
    props.map(KafkaConfig.fromProps)
  }

  @Test
  def testDeleteRecordsRequest(): Unit = {
    val topic = "topic-1"
    val request = new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
          .setTopicNames(Collections.singletonList(topic))
          .setTimeoutMs(1000)).build()
    val response = sendDeleteTopicsRequest(request)
    assertEquals(Errors.TOPIC_DELETION_DISABLED.code, response.data.responses.find(topic).errorCode)

    val v2request = new DeleteTopicsRequest.Builder(
        new DeleteTopicsRequestData()
        .setTopicNames(Collections.singletonList(topic))
        .setTimeoutMs(1000)).build(2)
    val v2response = sendDeleteTopicsRequest(v2request)
    assertEquals(Errors.INVALID_REQUEST.code, v2response.data.responses.find(topic).errorCode)
  }

  private def sendDeleteTopicsRequest(request: DeleteTopicsRequest): DeleteTopicsResponse = {
    connectAndReceive[DeleteTopicsResponse](request, destination = controllerSocketServer)
  }

  @Test
  def testDeletionDynamicFlag(): Unit = {
    val topic = "topic-1"
    val admin = createAdminClient()
    // Prepare:
    // 1. Config has topic deletion disabled
    // 2. Create topic
    // 3. Wait until topic is present
    admin.createTopics(List(new NewTopic(topic, 1, brokerCount.toShort)).asJava)
    TestUtils.waitUntilTopicPresent(admin, topic)

    // Execute:
    // 1. Set topic deletion flag
    // 2. Delete topic
    zkClient.setTopicDeletionFlag("true")
    val request = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData()
        .setTopicNames(Collections.singletonList(topic))
        .setTimeoutMs(1000)).build()
    val response = sendDeleteTopicsRequest(request)

    // Assert:
    // 1. Delete should be enabled
    // 2. Topic should eventually be deleted
    assertEquals(Errors.NONE, Errors.forCode(response.data.responses.find(topic).errorCode()))
    TestUtils.waitUntilTopicNotPresent(admin, topic)
  }

}
