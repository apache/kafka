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

import kafka.network.SocketServer
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse}
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

/**
 * see DescribeUserScramCredentialsRequestTest
 */
class DescribeUserScramCredentialsRequestNotAuthorizedTest extends BaseRequestTest {
  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")
    properties.put(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[StandardAuthorizer].getName)
    properties.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[DescribeCredentialsTest.TestPrincipalBuilderReturningUnauthorized].getName)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDescribeNotAuthorized(quorum: String): Unit = {
    val request = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response = sendDescribeUserScramCredentialsRequest(request)

    val error = response.data.errorCode
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, error, "Expected not authorized error")
  }

  private def sendDescribeUserScramCredentialsRequest(request: DescribeUserScramCredentialsRequest, socketServer: SocketServer = adminSocketServer): DescribeUserScramCredentialsResponse = {
    connectAndReceive[DescribeUserScramCredentialsResponse](request, destination = socketServer)
  }
}
