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

package unit.kafka.server

import java.net.InetAddress
import java.util.Properties

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.{ClientQuotaManager, ClientRequestQuotaManager, ControllerApis, ControllerMutationQuotaManager, KafkaConfig, MetaProperties, ReplicationQuotaManager}
import kafka.utils.MockTime
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.BrokerRegistrationRequestData
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, BrokerRegistrationRequest, BrokerRegistrationResponse, RequestContext, RequestHeader, RequestTestUtils}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.ApiMessageAndVersion
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult, Authorizer}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import scala.jdk.CollectionConverters._

class ControllerApisTest {
  private val nodeId = 1
  private val brokerRack = "Rack1"
  private val clientID = "Client1"
  private val requestChannelMetrics: RequestChannel.Metrics = mock(classOf[RequestChannel.Metrics])
  private val requestChannel: RequestChannel = mock(classOf[RequestChannel])
  private val time = new MockTime
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val raftManager: RaftManager[ApiMessageAndVersion] = mock(classOf[RaftManager[ApiMessageAndVersion]])
  private val authorizer: Authorizer = mock(classOf[Authorizer])

  private val quotas = QuotaManagers(
    clientQuotaManager,
    clientQuotaManager,
    clientRequestQuotaManager,
    clientControllerQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    None)
  private val controller: Controller = mock(classOf[Controller])

  private def createControllerApis(): ControllerApis = {
    val props = new Properties()
    props.put(KafkaConfig.NodeIdProp, nodeId: java.lang.Integer)
    props.put(KafkaConfig.ProcessRolesProp, "controller")
    new ControllerApis(
      requestChannel,
      Some(authorizer),
      quotas,
      time,
      Map.empty,
      controller,
      raftManager,
      new KafkaConfig(props),
      MetaProperties(Uuid.fromString("JgxuGe9URy-E-ceaL04lEw"), nodeId = nodeId),
      Seq.empty
    )
  }

  /**
   * Build a RequestChannel.Request from the AbstractRequest
   *
   * @param request - AbstractRequest
   * @param listenerName - Default listener for the RequestChannel
   * @tparam T - Type of AbstractRequest
   * @return
   */
  private def buildRequest[T <: AbstractRequest](
    request: AbstractRequest,
    listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  ): RequestChannel.Request = {
    val buffer = RequestTestUtils.serializeRequestWithHeader(
      new RequestHeader(request.apiKey, request.version, clientID, 0), request)

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics)
  }

  @Test
  def testUnauthorizedBrokerRegistration(): Unit = {
    val brokerRegistrationRequest = new BrokerRegistrationRequest.Builder(
      new BrokerRegistrationRequestData()
        .setBrokerId(nodeId)
        .setRack(brokerRack)
    ).build()

    val request = buildRequest(brokerRegistrationRequest)
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])

    when(authorizer.authorize(
      any(classOf[AuthorizableRequestContext]),
      any(classOf[java.util.List[Action]])
    )).thenReturn(
      java.util.Collections.singletonList(AuthorizationResult.DENIED)
    )

    createControllerApis().handle(request)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))

    assertNotNull(capturedResponse.getValue)

    val brokerRegistrationResponse = capturedResponse.getValue.asInstanceOf[BrokerRegistrationResponse]
    assertEquals(Map(Errors.CLUSTER_AUTHORIZATION_FAILED -> 1),
      brokerRegistrationResponse.errorCounts().asScala)
  }

  @AfterEach
  def tearDown(): Unit = {
    quotas.shutdown()
  }
}
