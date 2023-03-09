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
package kafka.server

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.common.message.{ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith

import java.io.EOFException

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.ALL, brokers = 1)
@Tag("integration")
class ConsumerGroupHeartbeatRequestTest(cluster: ClusterInstance) {

  @ClusterTest
  def testConsumerGroupHeartbeatIsDisabledByDefault(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
    ).build()
    assertThrows(classOf[EOFException], () => connectAndReceive(consumerGroupHeartbeatRequest))
  }

  @ClusterTest(serverProperties = Array(new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true")))
  def testConsumerGroupHeartbeatIsAccessibleWhenEnabled(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
    ).build()

    val consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
    val expectedResponse = new ConsumerGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, consumerGroupHeartbeatResponse.data)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true")
  ))
  def testConsumerGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabled(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
    ).build()

    val consumerGroupHeartbeatResponse = connectAndReceive(consumerGroupHeartbeatRequest)
    val expectedResponse = new ConsumerGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, consumerGroupHeartbeatResponse.data)
  }

  private def connectAndReceive(request: ConsumerGroupHeartbeatRequest): ConsumerGroupHeartbeatResponse = {
    IntegrationTestUtils.connectAndReceive[ConsumerGroupHeartbeatResponse](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}
