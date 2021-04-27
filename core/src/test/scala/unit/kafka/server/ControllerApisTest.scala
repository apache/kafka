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
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.Uuid.ZERO_UUID
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult
import org.apache.kafka.common.message._
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResourceCollection, AlterableConfig, AlterableConfigCollection, AlterConfigsResource}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResourceCollection => OldAlterConfigsResourceCollection}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => OldAlterConfigsResource}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterableConfigCollection => OldAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterableConfig => OldAlterableConfig}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => OldAlterConfigsResourceResponse}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests._
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

  def createDenyAllAuthorizer(): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    mock(classOf[Authorizer])
    when(authorizer.authorize(
      any(classOf[AuthorizableRequestContext]),
      any(classOf[java.util.List[Action]])
    )).thenReturn(
      java.util.Collections.singletonList(AuthorizationResult.DENIED)
    )
    authorizer
  }

  @Test
  def testUnauthorizedFetch(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleFetch(buildRequest(new FetchRequest(new FetchRequestData(), 12))))
  }

  @Test
  def testUnauthorizedVote(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleVote(buildRequest(new VoteRequest.Builder(new VoteRequestData()).build(0))))
  }

  @Test
  def testHandleLegacyAlterConfigsErrors(): Unit = {
    val requestData = new AlterConfigsRequestData().setResources(
      new OldAlterConfigsResourceCollection(util.Arrays.asList(
        new OldAlterConfigsResource().
          setResourceName("1").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(KafkaConfig.LogCleanerBackoffMsProp).
            setValue("100000")).iterator())),
        new OldAlterConfigsResource().
          setResourceName("2").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(KafkaConfig.LogCleanerBackoffMsProp).
            setValue("100000")).iterator())),
        new OldAlterConfigsResource().
          setResourceName("2").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(KafkaConfig.LogCleanerBackoffMsProp).
            setValue("100000")).iterator())),
        new OldAlterConfigsResource().
          setResourceName("baz").
          setResourceType(123.toByte).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName("foo").
            setValue("bar")).iterator())),
        ).iterator()))
    val request = buildRequest(new AlterConfigsRequest(requestData, 0))
    createControllerApis(Some(createDenyAllAuthorizer()),
      new MockController.Builder().build()).handleLegacyAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    assertNotNull(capturedResponse.getValue)
    val response = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    assertEquals(Set(
      new OldAlterConfigsResourceResponse().
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate resource.").
        setResourceName("2").
        setResourceType(ConfigResource.Type.BROKER.id()),
      new OldAlterConfigsResourceResponse().
        setErrorCode(UNSUPPORTED_VERSION.code()).
        setErrorMessage("Unknown resource type 123.").
        setResourceName("baz").
        setResourceType(123.toByte),
      new OldAlterConfigsResourceResponse().
        setErrorCode(CLUSTER_AUTHORIZATION_FAILED.code()).
        setErrorMessage("Cluster authorization failed.").
        setResourceName("1").
        setResourceType(ConfigResource.Type.BROKER.id())),
      response.data().responses().asScala.toSet)
  }

  @Test
  def testUnauthorizedBeginQuorumEpoch(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleBeginQuorumEpoch(buildRequest(new BeginQuorumEpochRequest.Builder(
          new BeginQuorumEpochRequestData()).build(0))))
  }

  @Test
  def testUnauthorizedEndQuorumEpoch(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleEndQuorumEpoch(buildRequest(new EndQuorumEpochRequest.Builder(
          new EndQuorumEpochRequestData()).build(0))))
  }

  @Test
  def testUnauthorizedDescribeQuorum(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleDescribeQuorum(buildRequest(new DescribeQuorumRequest.Builder(
          new DescribeQuorumRequestData()).build(0))))
  }

  @Test
  def testUnauthorizedHandleAlterIsrRequest(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleAlterIsrRequest(buildRequest(new AlterIsrRequest.Builder(
          new AlterIsrRequestData()).build(0))))
  }

  @Test
  def testUnauthorizedHandleBrokerHeartBeatRequest(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleBrokerHeartBeatRequest(buildRequest(new BrokerHeartbeatRequest.Builder(
          new BrokerHeartbeatRequestData()).build(0))))
  }

  @Test
  def testUnauthorizedHandleUnregisterBroker(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleUnregisterBroker(buildRequest(new UnregisterBrokerRequest.Builder(
          new UnregisterBrokerRequestData()).build(0))))
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

    createControllerApis(Some(createDenyAllAuthorizer()), mock(classOf[Controller])).handle(request)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))

    assertNotNull(capturedResponse.getValue)

    val brokerRegistrationResponse = capturedResponse.getValue.asInstanceOf[BrokerRegistrationResponse]
    assertEquals(Map(CLUSTER_AUTHORIZATION_FAILED -> 1),
      brokerRegistrationResponse.errorCounts().asScala)
  }

  @Test
  def testUnauthorizedHandleAlterClientQuotas(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleAlterClientQuotas(buildRequest(new AlterClientQuotasRequest(
          new AlterClientQuotasRequestData(), 0))))
  }

  @Test
  def testUnauthorizedHandleIncrementalAlterConfigs(): Unit = {
    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(
        util.Arrays.asList(new AlterConfigsResource().
          setResourceName("1").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(KafkaConfig.LogCleanerBackoffMsProp).
            setValue("100000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("foo").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.FLUSH_MS_CONFIG).
            setValue("1000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))
    createControllerApis(Some(createDenyAllAuthorizer()),
      new MockController.Builder().build()).handleIncrementalAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    assertNotNull(capturedResponse.getValue)
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    assertEquals(Set(new AlterConfigsResourceResponse().
        setErrorCode(CLUSTER_AUTHORIZATION_FAILED.code()).
        setErrorMessage(CLUSTER_AUTHORIZATION_FAILED.message()).
        setResourceName("1").
        setResourceType(ConfigResource.Type.BROKER.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message()).
        setResourceName("foo").
        setResourceType(ConfigResource.Type.TOPIC.id())),
      response.data().responses().asScala.toSet)
  }

  @Test
  def testInvalidIncrementalAlterConfigsResources(): Unit = {
    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName("1").
          setResourceType(ConfigResource.Type.BROKER_LOGGER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("kafka.server.KafkaConfig").
            setValue("TRACE").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("3").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(KafkaConfig.LogCleanerBackoffMsProp).
            setValue("100000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("3").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(KafkaConfig.LogCleanerBackoffMsProp).
            setValue("100000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("foo").
          setResourceType(124.toByte).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("foo").
            setValue("bar").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))
    createControllerApis(Some(createDenyAllAuthorizer()),
      new MockController.Builder().build()).handleIncrementalAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    assertNotNull(capturedResponse.getValue)
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    assertEquals(Set(
      new AlterConfigsResourceResponse().
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Unexpected resource type BROKER_LOGGER.").
        setResourceName("1").
        setResourceType(ConfigResource.Type.BROKER_LOGGER.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate resource.").
        setResourceName("3").
        setResourceType(ConfigResource.Type.BROKER.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(UNSUPPORTED_VERSION.code()).
        setErrorMessage("Unknown resource type 124.").
        setResourceName("foo").
        setResourceType(124.toByte)),
      response.data().responses().asScala.toSet)
  }

  @Test
  def testUnauthorizedHandleAlterPartitionReassignments(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
        handleAlterPartitionReassignments(buildRequest(new AlterPartitionReassignmentsRequest.Builder(
            new AlterPartitionReassignmentsRequestData()).build())))
  }

  @Test
  def testUnauthorizedHandleListPartitionReassignments(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => createControllerApis(
      Some(createDenyAllAuthorizer()), new MockController.Builder().build()).
      handleListPartitionReassignments(buildRequest(new ListPartitionReassignmentsRequest.Builder(
        new ListPartitionReassignmentsRequestData()).build())))
  }

  @Test
  def testCreateTopics(): Unit = {
    val controller = new MockController.Builder().build()
    val controllerApis = createControllerApis(None, controller)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(new CreatableTopic().setName("foo").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("foo").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("bar").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("bar").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("bar").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("baz").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("quux").setNumPartitions(2).setReplicationFactor(3),
    ).iterator()))
    val expectedResponse = Set(new CreatableTopicResult().setName("foo").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new CreatableTopicResult().setName("bar").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new CreatableTopicResult().setName("baz").
        setErrorCode(NONE.code()).
        setTopicId(new Uuid(0L, 1L)),
      new CreatableTopicResult().setName("quux").
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()))
    assertEquals(expectedResponse, controllerApis.createTopics(request,
      false,
      _ => Set("baz")).get().topics().asScala.toSet)
  }

  @Test
  def testDeleteTopicsByName(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().newInitialTopic("foo", fooId).build()
    val controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("foo", "bar", "quux", "quux"))
    val expectedResponse = Set(new DeletableTopicResult().setName("quux").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new DeletableTopicResult().setName("bar").
        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
        setErrorMessage("This server does not host this topic-partition."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId))
    assertEquals(expectedResponse, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
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
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic id."),
      new DeletableTopicResult().setName(null).setTopicId(barId).
        setErrorCode(UNKNOWN_TOPIC_ID.code()).
        setErrorMessage("This server does not host this topic ID."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId))
    assertEquals(response, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
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
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Neither topic name nor id were specified."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("You may not specify both topic name and topic id."),
      new DeletableTopicResult().setName("bar").setTopicId(barId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("The provided topic name maps to an ID that was already supplied."),
      new DeletableTopicResult().setName("quux").setTopicId(ZERO_UUID).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new DeletableTopicResult().setName(null).setTopicId(bazId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic id."))
    assertEquals(response, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      false,
      names => names.toSet,
      names => names.toSet).get().asScala.toSet)
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
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("quux").setTopicId(ZERO_UUID).
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("baz").setTopicId(ZERO_UUID).
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("foo").setTopicId(fooId).
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message))
    assertEquals(response, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      false,
      _ => Set("foo", "baz"),
      _ => Set.empty).get().asScala.toSet)
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
        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code).
        setErrorMessage(UNKNOWN_TOPIC_OR_PARTITION.message),
      new DeletableTopicResult().setName("bar").
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName(null).setTopicId(barId).
        setErrorCode(UNKNOWN_TOPIC_ID.code).
        setErrorMessage(UNKNOWN_TOPIC_ID.message))
    assertEquals(expectedResponse, controllerApis.deleteTopics(request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      false,
      _ => Set("foo"),
      _ => Set.empty).get().asScala.toSet)
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
        _ => Set("foo", "bar")).get()).getCause.getClass)
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

  @Test
  def testCreatePartitionsRequest(): Unit = {
    val controller = new MockController.Builder().
      newInitialTopic("foo", Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")).
      newInitialTopic("bar", Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")).build()
    val controllerApis = createControllerApis(None, controller)
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic().setName("foo").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("baz").setAssignments(null).setCount(5))
    assertEquals(Set(new CreatePartitionsTopicResult().setName("foo").
        setErrorCode(NONE.code()).
        setErrorMessage(null),
      new CreatePartitionsTopicResult().setName("bar").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new CreatePartitionsTopicResult().setName("baz").
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()).
        setErrorMessage(null)),
      controllerApis.createPartitions(request, false, _ => Set("foo", "bar")).get().asScala.toSet)
  }

  @AfterEach
  def tearDown(): Unit = {
    quotas.shutdown()
  }
}
