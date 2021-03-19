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

import java.net.InetAddress
import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.test.MockController
import kafka.utils.MockTime
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.Uuid.ZERO_UUID
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidRequestException, NotControllerException, TopicDeletionDisabledException}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult
import org.apache.kafka.common.message.{BrokerRegistrationRequestData, DeleteTopicsRequestData}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, BrokerRegistrationRequest, RequestContext, RequestHeader, RequestTestUtils}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.ApiMessageAndVersion
import org.apache.kafka.server.authorizer.{AuthorizableRequestContext, AuthorizationResult, Authorizer}
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import scala.jdk.CollectionConverters._

class ControllerApisTest {
  // Mocks
  private val nodeId = 1
  private val brokerRack = "Rack1"
  private val clientID = "Client1"
  private val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
  private val requestChannel: RequestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val time = new MockTime
  private val clientQuotaManager: ClientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = EasyMock.createNiceMock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val raftManager: RaftManager[ApiMessageAndVersion] = EasyMock.createNiceMock(classOf[RaftManager[ApiMessageAndVersion]])

  private val quotas = QuotaManagers(
    clientQuotaManager,
    clientQuotaManager,
    clientRequestQuotaManager,
    clientControllerQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    None)

  private def createControllerApis(authorizer: Option[Authorizer],
                                   controller: Controller,
                                   props: Properties = new Properties()): ControllerApis = {
    props.put(KafkaConfig.NodeIdProp, nodeId: java.lang.Integer)
    props.put(KafkaConfig.ProcessRolesProp, "controller")
    new ControllerApis(
      requestChannel,
      authorizer,
      quotas,
      time,
      Map.empty,
      controller,
      raftManager,
      new KafkaConfig(props),

      // FIXME: Would make more sense to set controllerId here
      MetaProperties("JgxuGe9URy-E-ceaL04lEw", nodeId = nodeId),
      Seq.empty,
      new SimpleApiVersionManager(ListenerType.CONTROLLER)
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
        .setBrokerId(nodeId)
        .setRack(brokerRack)
    ).build()

    val request = buildRequest(brokerRegistrationRequest)

    val capturedResponse: Capture[RequestChannel.Response] = EasyMock.newCapture()
    EasyMock.expect(requestChannel.sendResponse(EasyMock.capture(capturedResponse)))

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

    val controllerApis = createControllerApis(authorizer = authorizer, EasyMock.createNiceMock(classOf[Controller]))
    val assertion = assertThrows(classOf[ClusterAuthorizationException],
      () => controllerApis.handleBrokerRegistration(request))
    assert(Errors.forException(assertion) == Errors.CLUSTER_AUTHORIZATION_FAILED)
  }

  @Test
  def testDeleteTopicsByName(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().newInitialTopic("foo", fooId).build()
    val controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("foo", "bar", "quux", "quux"))
    val expectedResponse = Set(new DeletableTopicResult().setName("quux").
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new DeletableTopicResult().setName("bar").
        setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()).
        setErrorMessage("This server does not host this topic-partition."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId))
    assertEquals(expectedResponse, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      true,
      _ => Set.empty,
      _ => Set.empty).asScala.toSet)
  }

  @Test
  def testDeleteTopicsById(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val quuxId = Uuid.fromString("ObXkLhL_S5W62FAE67U3MQ")
    val controller = new MockController.Builder().newInitialTopic("foo", fooId).build()
    val controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(quuxId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(quuxId))
    val response = Set(new DeletableTopicResult().setName(null).setTopicId(quuxId).
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic id."),
      new DeletableTopicResult().setName(null).setTopicId(barId).
        setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()).
        setErrorMessage("This server does not host this topic ID."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId))
    assertEquals(response, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      true,
      _ => Set.empty,
      _ => Set.empty).asScala.toSet)
  }

  @Test
  def testInvalidDeleteTopicsRequest(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val bazId = Uuid.fromString("YOS4oQ3UT9eSAZahN1ysSA")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).
      newInitialTopic("bar", barId).build()
    val controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("foo").setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName("bar").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(bazId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(bazId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(bazId))
    val response = Set(new DeletableTopicResult().setName(null).setTopicId(ZERO_UUID).
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Neither topic name nor id were specified."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId).
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("You may not specify both topic name and topic id."),
      new DeletableTopicResult().setName("bar").setTopicId(barId).
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("The provided topic name maps to an ID that was already supplied."),
      new DeletableTopicResult().setName("quux").setTopicId(ZERO_UUID).
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new DeletableTopicResult().setName(null).setTopicId(bazId).
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic id."))
    assertEquals(response, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      false,
      names => names.toSet,
      names => names.toSet).asScala.toSet)
  }

  @Test
  def testNotAuthorizedToDeleteWithTopicExisting(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val bazId = Uuid.fromString("hr4TVh3YQiu3p16Awkka6w")
    val quuxId = Uuid.fromString("5URoQzW_RJiERVZXJgUVLg")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).
      newInitialTopic("bar", barId).
      newInitialTopic("baz", bazId).
      newInitialTopic("quux", quuxId).build()
    val controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    request.topics().add(new DeleteTopicState().setName("baz").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    val response = Set(new DeletableTopicResult().setName(null).setTopicId(barId).
        setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("quux").setTopicId(ZERO_UUID).
        setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("baz").setTopicId(ZERO_UUID).
        setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("foo").setTopicId(fooId).
        setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message))
    assertEquals(response, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      false,
      _ => Set("foo", "baz"),
      _ => Set.empty).asScala.toSet)
  }

  @Test
  def testNotAuthorizedToDeleteWithTopicNotExisting(): Unit = {
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val controller = new MockController.Builder().build()
    val controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName("foo").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("bar").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    val expectedResponse = Set(new DeletableTopicResult().setName("foo").
        setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code).
        setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message),
      new DeletableTopicResult().setName("bar").
        setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName(null).setTopicId(barId).
        setErrorCode(Errors.UNKNOWN_TOPIC_ID.code).
        setErrorMessage(Errors.UNKNOWN_TOPIC_ID.message))
    assertEquals(expectedResponse, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      false,
      _ => Set("foo"),
      _ => Set.empty).asScala.toSet)
  }

  @Test
  def testNotControllerErrorPreventsDeletingTopics(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).build()
    controller.setActive(false)
    val controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    assertEquals(classOf[NotControllerException], assertThrows(
      classOf[ExecutionException], () => controllerApis.deleteTopics(request,
        ApiKeys.DELETE_TOPICS.latestVersion().toInt,
        false,
        _ => Set("foo", "bar"),
        _ => Set("foo", "bar"))).getCause.getClass)
  }

  @Test
  def testDeleteTopicsDisabled(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).build()
    val props = new Properties()
    props.put(KafkaConfig.DeleteTopicEnableProp, "false")
    val controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName("foo").setTopicId(ZERO_UUID))
    assertThrows(classOf[TopicDeletionDisabledException], () => controllerApis.deleteTopics(request,
        ApiKeys.DELETE_TOPICS.latestVersion().toInt,
        false,
        _ => Set("foo", "bar"),
        _ => Set("foo", "bar")))
    assertThrows(classOf[InvalidRequestException], () => controllerApis.deleteTopics(request,
        1,
        false,
        _ => Set("foo", "bar"),
        _ => Set("foo", "bar")))
  }

  @AfterEach
  def tearDown(): Unit = {
    quotas.shutdown()
  }
}
