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
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.BrokerRegistrationRequestData
import org.apache.kafka.common.network.{ClientInformation, ListenerName, Send}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, BrokerRegistrationRequest, RequestContext, RequestHeader, RequestTestUtils}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.VersionRange
import org.apache.kafka.server.authorizer.{AuthorizableRequestContext, AuthorizationResult, Authorizer}
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Test}
import org.scalatest.Matchers.intercept

class ControllerApisTest {
  // Mocks
  private val brokerId = 1
  private val brokerRack = "Rack1"
  private val clientID = "Client1"
  private val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
  private val requestChannel: RequestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val time = new MockTime
  private val clientQuotaManager: ClientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = EasyMock.createNiceMock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val raftManager: RaftManager = EasyMock.createNiceMock(classOf[RaftManager])
  private val quotas = QuotaManagers(
    clientQuotaManager,
    clientQuotaManager,
    clientRequestQuotaManager,
    clientControllerQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    None)
  private val controller: Controller = EasyMock.createNiceMock(classOf[Controller])

  private def createControllerApis(authorizer: Option[Authorizer],
                                   supportedFeatures: Map[String, VersionRange] = Map.empty): ControllerApis = {
    new ControllerApis(
      requestChannel,
      authorizer,
      quotas,
      time,
      supportedFeatures,
      controller,
      Some(raftManager),
      new KafkaConfig(new Properties()),

      // FIXME: Would make more sense to set controllerId here
      MetaProperties(Uuid.fromString("JgxuGe9URy-E-ceaL04lEw"), brokerId = Some(brokerId)),
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
  private def buildRequest[T <: AbstractRequest](request: AbstractRequest,
                                                 listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): RequestChannel.Request = {
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
  def testBrokerRegistration(): Unit = {
    val brokerRegistrationRequest = new BrokerRegistrationRequest.Builder(
      new BrokerRegistrationRequestData()
        .setBrokerId(brokerId)
        .setRack(brokerRack)
    ).build()

    val request = buildRequest(brokerRegistrationRequest)

    val capturedResponse: Capture[Option[AbstractResponse]] = EasyMock.newCapture()
    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.anyObject[RequestChannel.Request](),
      EasyMock.capture(capturedResponse),
      EasyMock.anyObject[Option[Send => Unit]]()
    ))

    val authorizer = Some[Authorizer](EasyMock.createNiceMock(classOf[Authorizer]))
    EasyMock.expect(authorizer.get.authorize(EasyMock.anyObject[AuthorizableRequestContext](), EasyMock.anyObject())).andAnswer(
      new IAnswer[java.util.List[AuthorizationResult]]() {
        override def answer(): java.util.List[AuthorizationResult] = {
          new java.util.ArrayList[AuthorizationResult](){
            add(AuthorizationResult.DENIED)
          }
        }
      }
    )
    EasyMock.replay(requestChannel, authorizer.get)

    val assertion = intercept[ClusterAuthorizationException] {
      createControllerApis(authorizer = authorizer).handleBrokerRegistration(request)
    }
    assert(Errors.forException(assertion) == Errors.CLUSTER_AUTHORIZATION_FAILED)
  }

  @After
  def tearDown(): Unit = {
    quotas.shutdown()
  }
}
