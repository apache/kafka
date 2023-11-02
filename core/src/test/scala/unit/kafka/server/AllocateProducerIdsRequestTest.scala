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
package unit.kafka.server

import kafka.network.SocketServer
import kafka.server.{BrokerServer, ControllerServer, IntegrationTestUtils}
import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance
import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.server.common.ProducerIdsBlock
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{Tag, Timeout}

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT)
@Tag("integration")
class AllocateProducerIdsRequestTest(cluster: ClusterInstance) {

  @ClusterTest
  def testAllocateProducersIdSentToController(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val sourceBroker = raftCluster.brokers.findFirst().get()

    val controllerId = sourceBroker.raftManager.leaderAndEpoch.leaderId().getAsInt
    val controllerServer = raftCluster.controllers()
      .filter(_.config.nodeId == controllerId)
      .findFirst()
      .get()

    val allocateResponse = sendAndReceiveAllocateProducerIds(sourceBroker, controllerServer)
    assertEquals(Errors.NONE, allocateResponse.error)
    assertEquals(ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE, allocateResponse.data.producerIdLen)
    assertTrue(allocateResponse.data.producerIdStart >= 0)
  }

  @ClusterTest(controllers = 3)
  def testAllocateProducersIdSentToNonController(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val sourceBroker = raftCluster.brokers.findFirst().get()

    val controllerId = sourceBroker.raftManager.leaderAndEpoch.leaderId().getAsInt
    val controllerServer = raftCluster.controllers()
      .filter(_.config.nodeId != controllerId)
      .findFirst()
      .get()

    val allocateResponse = sendAndReceiveAllocateProducerIds(sourceBroker, controllerServer)
    assertEquals(Errors.NOT_CONTROLLER, Errors.forCode(allocateResponse.data.errorCode))
  }

  private def sendAndReceiveAllocateProducerIds(
    sourceBroker: BrokerServer,
    controllerServer: ControllerServer
  ): AllocateProducerIdsResponse = {
    val allocateRequest = new AllocateProducerIdsRequest.Builder(
      new AllocateProducerIdsRequestData()
        .setBrokerId(sourceBroker.config.brokerId)
        .setBrokerEpoch(sourceBroker.lifecycleManager.brokerEpoch)
    ).build()

    connectAndReceive(
      controllerServer.socketServer,
      allocateRequest
    )
  }

  private def connectAndReceive(
    controllerSocketServer: SocketServer,
    request: AllocateProducerIdsRequest
  ): AllocateProducerIdsResponse = {
    IntegrationTestUtils.connectAndReceive[AllocateProducerIdsResponse](
      request,
      controllerSocketServer,
      cluster.controllerListenerName.get
    )
  }

}
