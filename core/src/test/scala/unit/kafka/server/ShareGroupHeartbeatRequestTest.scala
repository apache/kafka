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
import kafka.test.annotation.{ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.common.message.{ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse}
import org.junit.jupiter.api.Assertions.{assertEquals}
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class ShareGroupHeartbeatRequestTest(cluster: ClusterInstance) {

  @ClusterTest()
  def testShareGroupHeartbeatIsAccessibleWhenEnabled(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData(), true
    ).build()

    val shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
    val expectedResponse = new ShareGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, shareGroupHeartbeatResponse.data)
  }

  private def connectAndReceive(request: ShareGroupHeartbeatRequest): ShareGroupHeartbeatResponse = {
    IntegrationTestUtils.connectAndReceive[ShareGroupHeartbeatResponse](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}
