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

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Collections, Optional, Properties}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, NodeApiVersions, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME}
import org.apache.kafka.common.message.{ApiVersionsResponseData, CreateTopicsRequestData}
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicConfig, CreatableTopicConfigCollection}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, ByteBufferAccessor, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.RequestUtils
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.{SecurityUtils, Utils}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.coordinator.group.{GroupCoordinator, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorConfig}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.quota.ControllerMutationQuota
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.Mockito.never

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class AutoTopicCreationManagerTest {

  private val requestTimeout = 100
  private val testCacheCapacity = 3
  private var config: KafkaConfig = _
  private val metadataCache = Mockito.mock(classOf[MetadataCache])
  private val brokerToController = Mockito.mock(classOf[NodeToControllerChannelManager])
  private val groupCoordinator = Mockito.mock(classOf[GroupCoordinator])
  private val transactionCoordinator = Mockito.mock(classOf[TransactionCoordinator])
  private val shareCoordinator = Mockito.mock(classOf[ShareCoordinator])
  private var autoTopicCreationManager: AutoTopicCreationManager = _
  private val mockTime = new MockTime(0L, 0L)

  private val internalTopicPartitions = 2
  private val internalTopicReplicationFactor: Short = 2

  @BeforeEach
  def setup(): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    props.setProperty(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toString)

    props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, internalTopicPartitions.toString)
    props.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, internalTopicPartitions.toString)
    props.setProperty(ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG , internalTopicPartitions.toString)

    props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, internalTopicReplicationFactor.toString)
    props.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, internalTopicReplicationFactor.toString)
    props.setProperty(ShareCoordinatorConfig.STATE_TOPIC_NUM_PARTITIONS_CONFIG, internalTopicReplicationFactor.toString)
    // Set a short group max session timeout for testing TTL (1 second)
    props.setProperty(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, "1000")

    config = KafkaConfig.fromProps(props)
    val aliveBrokers = util.List.of(new Node(0, "host0", 0), new Node(1, "host1", 1))

    Mockito.when(metadataCache.getAliveBrokerNodes(any(classOf[ListenerName]))).thenReturn(aliveBrokers)
  }

  @Test
  def testCreateOffsetTopic(): Unit = {
    Mockito.when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
    testCreateTopic(GROUP_METADATA_TOPIC_NAME, isInternal = true, internalTopicPartitions, internalTopicReplicationFactor)
  }

  @Test
  def testCreateTxnTopic(): Unit = {
    Mockito.when(transactionCoordinator.transactionTopicConfigs).thenReturn(new Properties)
    testCreateTopic(TRANSACTION_STATE_TOPIC_NAME, isInternal = true, internalTopicPartitions, internalTopicReplicationFactor)
  }

  @Test
  def testCreateShareStateTopic(): Unit = {
    Mockito.when(shareCoordinator.shareGroupStateTopicConfigs()).thenReturn(new Properties)
    testCreateTopic(SHARE_GROUP_STATE_TOPIC_NAME, isInternal = true, internalTopicPartitions, internalTopicReplicationFactor)
  }

  @Test
  def testCreateNonInternalTopic(): Unit = {
    testCreateTopic("topic", isInternal = false)
  }

  private def testCreateTopic(topicName: String,
                              isInternal: Boolean,
                              numPartitions: Int = 1,
                              replicationFactor: Short = 1): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection
    topicsCollection.add(getNewTopic(topicName, numPartitions, replicationFactor))
    val requestBody = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTopics(topicsCollection)
        .setTimeoutMs(requestTimeout))

    // Calling twice with the same topic will only trigger one forwarding.
    createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal)
    createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal)

    Mockito.verify(brokerToController).sendRequest(
      ArgumentMatchers.eq(requestBody),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  @Test
  def testTopicCreationWithMetadataContextPassPrincipal(): Unit = {
    val topicName = "topic"

    val userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user")
    val serializeIsCalled = new AtomicBoolean(false)
    val principalSerde = new KafkaPrincipalSerde {
      override def serialize(principal: KafkaPrincipal): Array[Byte] = {
        assertEquals(principal, userPrincipal)
        serializeIsCalled.set(true)
        Utils.utf8(principal.toString)
      }
      override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
    }

    val requestContext = initializeRequestContext(userPrincipal, Optional.of(principalSerde))

    autoTopicCreationManager.createTopics(
      Set(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, Some(requestContext))

    assertTrue(serializeIsCalled.get())

    val argumentCaptor = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[_ <: AbstractRequest]])
    Mockito.verify(brokerToController).sendRequest(
      argumentCaptor.capture(),
      any(classOf[ControllerRequestCompletionHandler]))
    val capturedRequest = argumentCaptor.getValue.asInstanceOf[EnvelopeRequest.Builder].build(ApiKeys.ENVELOPE.latestVersion())
    assertEquals(userPrincipal, SecurityUtils.parseKafkaPrincipal(Utils.utf8(capturedRequest.requestPrincipal)))
  }

  @Test
  def testTopicCreationWithMetadataContextWhenPrincipalSerdeNotDefined(): Unit = {
    val topicName = "topic"

    val requestContext = initializeRequestContext(KafkaPrincipal.ANONYMOUS, Optional.empty())

    // Throw upon undefined principal serde when building the forward request
    assertThrows(classOf[IllegalArgumentException], () => autoTopicCreationManager.createTopics(
      Set(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, Some(requestContext)))
  }

  @Test
  def testTopicCreationWithMetadataContextNoRetryUponUnsupportedVersion(): Unit = {
    val topicName = "topic"

    val principalSerde = new KafkaPrincipalSerde {
      override def serialize(principal: KafkaPrincipal): Array[Byte] = {
        Utils.utf8(principal.toString)
      }
      override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
    }

    val requestContext = initializeRequestContext(KafkaPrincipal.ANONYMOUS, Optional.of(principalSerde))
    autoTopicCreationManager.createTopics(
      Set(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, Some(requestContext))
    autoTopicCreationManager.createTopics(
      Set(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, Some(requestContext))

    // Should only trigger once
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Complete with unsupported version will not trigger a retry, but cleanup the inflight topics instead
    val header = new RequestHeader(ApiKeys.ENVELOPE, 0, "client", 1)
    val response = new EnvelopeResponse(ByteBuffer.allocate(0), Errors.UNSUPPORTED_VERSION)
    val clientResponse = new ClientResponse(header, null, null,
      0, 0, false, null, null, response)
    argumentCaptor.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
    Mockito.verify(brokerToController, Mockito.times(1)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Could do the send again as inflight topics are cleared.
    autoTopicCreationManager.createTopics(
      Set(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, Some(requestContext))
    Mockito.verify(brokerToController, Mockito.times(2)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())
  }

  @Test
  def testCreateStreamsInternalTopics(): Unit = {
    val topicConfig = new CreatableTopicConfigCollection()
    topicConfig.add(new CreatableTopicConfig().setName("cleanup.policy").setValue("compact"))

    val topics = Map(
      "stream-topic-1" -> new CreatableTopic().setName("stream-topic-1").setNumPartitions(3).setReplicationFactor(2).setConfigs(topicConfig),
      "stream-topic-2" -> new CreatableTopic().setName("stream-topic-2").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()

    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[_ <: AbstractRequest]])
    Mockito.verify(brokerToController).sendRequest(
      argumentCaptor.capture(),
      any(classOf[ControllerRequestCompletionHandler]))

    val requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, ApiKeys.CREATE_TOPICS.latestVersion(), "clientId", 0)
    val capturedRequest = argumentCaptor.getValue.asInstanceOf[EnvelopeRequest.Builder].build(ApiKeys.ENVELOPE.latestVersion())
    val topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection
    topicsCollection.add(getNewTopic("stream-topic-1", 3, 2.toShort).setConfigs(topicConfig))
    topicsCollection.add(getNewTopic("stream-topic-2", 1, 1.toShort))
    val requestBody = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTopics(topicsCollection)
        .setTimeoutMs(requestTimeout))
      .build(ApiKeys.CREATE_TOPICS.latestVersion())

    val forwardedRequestBuffer = capturedRequest.requestData().duplicate()
    assertEquals(requestHeader, RequestHeader.parse(forwardedRequestBuffer))
    assertEquals(requestBody.data(), CreateTopicsRequest.parse(new ByteBufferAccessor(forwardedRequestBuffer),
      ApiKeys.CREATE_TOPICS.latestVersion()).data())
  }

  @Test
  def testCreateStreamsInternalTopicsWithEmptyTopics(): Unit = {
    val topics = Map.empty[String, CreatableTopic]
    val requestContext = initializeRequestContextWithUserPrincipal()

    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    Mockito.verify(brokerToController, never()).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  @Test
  def testCreateStreamsInternalTopicsPassesPrincipal(): Unit = {
    val topics = Map(
      "stream-topic-1" -> new CreatableTopic().setName("stream-topic-1").setNumPartitions(-1).setReplicationFactor(-1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()

    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[_ <: AbstractRequest]])
    Mockito.verify(brokerToController).sendRequest(
      argumentCaptor.capture(),
      any(classOf[ControllerRequestCompletionHandler]))
    val capturedRequest = argumentCaptor.getValue.asInstanceOf[EnvelopeRequest.Builder].build(ApiKeys.ENVELOPE.latestVersion())
    assertEquals(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"), SecurityUtils.parseKafkaPrincipal(Utils.utf8(capturedRequest.requestPrincipal)))
  }

  private def initializeRequestContextWithUserPrincipal(): RequestContext = {
    val userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user")
    val principalSerde = new KafkaPrincipalSerde {
      override def serialize(principal: KafkaPrincipal): Array[Byte] = {
        Utils.utf8(principal.toString)
      }
      override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
    }
    initializeRequestContext(userPrincipal, Optional.of(principalSerde))
  }

  private def initializeRequestContext(kafkaPrincipal: KafkaPrincipal,
                                       principalSerde: Optional[KafkaPrincipalSerde]): RequestContext = {

    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val createTopicApiVersion = new ApiVersionsResponseData.ApiVersion()
      .setApiKey(ApiKeys.CREATE_TOPICS.id)
      .setMinVersion(ApiKeys.CREATE_TOPICS.oldestVersion())
      .setMaxVersion(ApiKeys.CREATE_TOPICS.latestVersion())
    Mockito.when(brokerToController.controllerApiVersions())
      .thenReturn(Optional.of(NodeApiVersions.create(Collections.singleton(createTopicApiVersion))))

    val requestHeader = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion,
      "clientId", 0)
    new RequestContext(requestHeader, "1", InetAddress.getLocalHost, Optional.empty(),
      kafkaPrincipal, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false, principalSerde)
  }

  private def createTopicAndVerifyResult(error: Errors,
                                         topicName: String,
                                         isInternal: Boolean,
                                         metadataContext: Option[RequestContext] = None): Unit = {
    val topicResponses = autoTopicCreationManager.createTopics(
      Set(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, metadataContext)

    val expectedResponses = Seq(new MetadataResponseTopic()
      .setErrorCode(error.code())
      .setIsInternal(isInternal)
      .setName(topicName))

    assertEquals(expectedResponses, topicResponses)
  }

  private def getNewTopic(topicName: String, numPartitions: Int, replicationFactor: Short): CreatableTopic = {
    new CreatableTopic()
      .setName(topicName)
      .setNumPartitions(numPartitions)
      .setReplicationFactor(replicationFactor)
  }

  @Test
  def testTopicCreationErrorCaching(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic-1" -> new CreatableTopic().setName("test-topic-1").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Simulate a CreateTopicsResponse with errors
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic-1")
      .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
      .setErrorMessage("Topic 'test-topic-1' already exists.")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val header = new RequestHeader(ApiKeys.CREATE_TOPICS, 0, "client", 1)
    val clientResponse = new ClientResponse(header, null, null,
      0, 0, false, null, null, createTopicsResponse)

    // Trigger the completion handler
    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify that the error was cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic-1"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)
    assertTrue(cachedErrors.contains("test-topic-1"))
    assertEquals("Topic 'test-topic-1' already exists.", cachedErrors("test-topic-1"))
  }

  @Test
  def testGetTopicCreationErrorsWithMultipleTopics(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "success-topic" -> new CreatableTopic().setName("success-topic").setNumPartitions(1).setReplicationFactor(1),
      "failed-topic" -> new CreatableTopic().setName("failed-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Simulate mixed response - one success, one failure
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    createTopicsResponseData.topics().add(
      new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
        .setName("success-topic")
        .setErrorCode(Errors.NONE.code())
    )
    createTopicsResponseData.topics().add(
      new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
        .setName("failed-topic")
        .setErrorCode(Errors.POLICY_VIOLATION.code())
        .setErrorMessage("Policy violation")
    )

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val header = new RequestHeader(ApiKeys.CREATE_TOPICS, 0, "client", 1)
    val clientResponse = new ClientResponse(header, null, null,
      0, 0, false, null, null, createTopicsResponse)

    argumentCaptor.getValue.onComplete(clientResponse)

    // Only the failed topic should be cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("success-topic", "failed-topic", "nonexistent-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)
    assertTrue(cachedErrors.contains("failed-topic"))
    assertEquals("Policy violation", cachedErrors("failed-topic"))
  }

  @Test 
  def testErrorCacheTTL(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)


    // First cache an error by simulating topic creation failure
    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val shortTtlMs = 1000L // Use 1 second TTL for faster testing
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Simulate a CreateTopicsResponse with error
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val header = new RequestHeader(ApiKeys.CREATE_TOPICS, 0, "client", 1)
    val clientResponse = new ClientResponse(header, null, null,
      0, 0, false, null, null, createTopicsResponse)

    // Cache the error at T0
    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify error is cached and accessible within TTL
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)
    assertEquals("Invalid replication factor", cachedErrors("test-topic"))

    // Advance time beyond TTL
    mockTime.sleep(shortTtlMs + 100) // T0 + 1.1 seconds

    // Verify error is now expired and proactively cleaned up
    val expiredErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertTrue(expiredErrors.isEmpty, "Expired errors should be proactively cleaned up")
  }

  @Test
  def testEnvelopeResponseSuccessfulParsing(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Create a successful CreateTopicsResponse
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.NONE.code())
      .setNumPartitions(1)
      .setReplicationFactor(1.toShort)
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val requestVersion = ApiKeys.CREATE_TOPICS.latestVersion()
    val correlationId = requestContext.correlationId // Use the actual correlation ID from request context
    val clientId = requestContext.clientId

    // Serialize the CreateTopicsResponse with header as it would appear in an envelope
    val responseHeader = new ResponseHeader(correlationId, ApiKeys.CREATE_TOPICS.responseHeaderVersion(requestVersion))
    val serializedResponse = RequestUtils.serialize(responseHeader.data(), responseHeader.headerVersion(), 
                                                     createTopicsResponse.data(), requestVersion)

    // Create an EnvelopeResponse containing the serialized CreateTopicsResponse
    val envelopeResponse = new EnvelopeResponse(serializedResponse, Errors.NONE)
    val requestHeader = new RequestHeader(ApiKeys.ENVELOPE, 0, clientId, correlationId)
    val clientResponse = new ClientResponse(requestHeader, null, null,
      0, 0, false, null, null, envelopeResponse)

    // Trigger the completion handler
    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify no errors were cached (successful response)
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertTrue(cachedErrors.isEmpty, "No errors should be cached for successful response")
  }

  @Test
  def testEnvelopeResponseWithEnvelopeError(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Create an EnvelopeResponse with an envelope-level error
    val envelopeResponse = new EnvelopeResponse(ByteBuffer.allocate(0), Errors.UNSUPPORTED_VERSION)
    val requestHeader = new RequestHeader(ApiKeys.ENVELOPE, 0, requestContext.clientId, requestContext.correlationId)
    val clientResponse = new ClientResponse(requestHeader, null, null,
      0, 0, false, null, null, envelopeResponse)

    // Trigger the completion handler
    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify the envelope error was cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)
    assertTrue(cachedErrors("test-topic").contains("Envelope error: UNSUPPORTED_VERSION"))
  }

  @Test
  def testEnvelopeResponseParsingException(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Create an EnvelopeResponse with malformed response data that will cause parsing to fail
    val malformedData = ByteBuffer.wrap("invalid response data".getBytes())
    val envelopeResponse = new EnvelopeResponse(malformedData, Errors.NONE)
    val requestHeader = new RequestHeader(ApiKeys.ENVELOPE, 0, requestContext.clientId, requestContext.correlationId)
    val clientResponse = new ClientResponse(requestHeader, null, null,
      0, 0, false, null, null, envelopeResponse)

    // Trigger the completion handler
    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify the parsing error was cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)
    assertTrue(cachedErrors("test-topic").contains("Response parsing error:"))
  }

  @Test
  def testEnvelopeResponseCorrelationIdMismatch(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Create a CreateTopicsResponse with a different correlation ID than the request
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.NONE.code())
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val requestVersion = ApiKeys.CREATE_TOPICS.latestVersion()
    val requestCorrelationId = 123
    val responseCorrelationId = 456 // Different correlation ID
    val clientId = "test-client"

    // Serialize the CreateTopicsResponse with mismatched correlation ID
    val responseHeader = new ResponseHeader(responseCorrelationId, ApiKeys.CREATE_TOPICS.responseHeaderVersion(requestVersion))
    val serializedResponse = RequestUtils.serialize(responseHeader.data(), responseHeader.headerVersion(),
                                                     createTopicsResponse.data(), requestVersion)

    // Create an EnvelopeResponse containing the serialized CreateTopicsResponse
    val envelopeResponse = new EnvelopeResponse(serializedResponse, Errors.NONE)
    val requestHeader = new RequestHeader(ApiKeys.ENVELOPE, 0, clientId, requestCorrelationId)
    val clientResponse = new ClientResponse(requestHeader, null, null,
      0, 0, false, null, null, envelopeResponse)

    // Trigger the completion handler
    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify the correlation ID mismatch error was cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)
    assertTrue(cachedErrors("test-topic").contains("Response parsing error:"))
  }

  @Test
  def testEnvelopeResponseWithTopicErrors(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic-1" -> new CreatableTopic().setName("test-topic-1").setNumPartitions(1).setReplicationFactor(1),
      "test-topic-2" -> new CreatableTopic().setName("test-topic-2").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Create a CreateTopicsResponse with mixed success and error results
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()

    // Successful topic
    val successResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic-1")
      .setErrorCode(Errors.NONE.code())
      .setNumPartitions(1)
      .setReplicationFactor(1.toShort)
    createTopicsResponseData.topics().add(successResult)

    // Failed topic
    val errorResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic-2")
      .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
      .setErrorMessage("Topic already exists")
    createTopicsResponseData.topics().add(errorResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val requestVersion = ApiKeys.CREATE_TOPICS.latestVersion()
    val correlationId = requestContext.correlationId  // Use the actual correlation ID from request context
    val clientId = requestContext.clientId

    // Serialize the CreateTopicsResponse with header
    val responseHeader = new ResponseHeader(correlationId, ApiKeys.CREATE_TOPICS.responseHeaderVersion(requestVersion))
    val serializedResponse = RequestUtils.serialize(responseHeader.data(), responseHeader.headerVersion(),
                                                     createTopicsResponse.data(), requestVersion)

    // Create an EnvelopeResponse containing the serialized CreateTopicsResponse
    val envelopeResponse = new EnvelopeResponse(serializedResponse, Errors.NONE)
    val requestHeader = new RequestHeader(ApiKeys.ENVELOPE, 0, clientId, correlationId)
    val clientResponse = new ClientResponse(requestHeader, null, null,
      0, 0, false, null, null, envelopeResponse)

    // Trigger the completion handler
    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify only the failed topic was cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
      Set("test-topic-1", "test-topic-2"), mockTime.milliseconds())
    
    assertEquals(1, cachedErrors.size, s"Expected only 1 error but found: $cachedErrors")
    assertTrue(cachedErrors.contains("test-topic-2"))
    assertEquals("Topic already exists", cachedErrors("test-topic-2"))
  }

  @Test
  def testSendCreateTopicRequestEnvelopeHandling(): Unit = {
    // Test the sendCreateTopicRequest method (without error caching) handles envelopes correctly
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val requestContext = initializeRequestContextWithUserPrincipal()

    // Call createTopics which uses sendCreateTopicRequest internally
    autoTopicCreationManager.createTopics(
      Set("test-topic"), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, Some(requestContext))

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Create a CreateTopicsResponse with an error
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
      .setErrorMessage("Invalid topic name")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val requestVersion = ApiKeys.CREATE_TOPICS.latestVersion()
    val correlationId = requestContext.correlationId  // Use the actual correlation ID from request context
    val clientId = requestContext.clientId

    // Serialize the CreateTopicsResponse with header
    val responseHeader = new ResponseHeader(correlationId, ApiKeys.CREATE_TOPICS.responseHeaderVersion(requestVersion))
    val serializedResponse = RequestUtils.serialize(responseHeader.data(), responseHeader.headerVersion(),
                                                     createTopicsResponse.data(), requestVersion)

    // Create an EnvelopeResponse containing the serialized CreateTopicsResponse
    val envelopeResponse = new EnvelopeResponse(serializedResponse, Errors.NONE)
    val requestHeader = new RequestHeader(ApiKeys.ENVELOPE, 0, clientId, correlationId)
    val clientResponse = new ClientResponse(requestHeader, null, null,
      0, 0, false, null, null, envelopeResponse)

    // Trigger the completion handler
    argumentCaptor.getValue.onComplete(clientResponse)

    // For sendCreateTopicRequest, errors are not cached, but we can verify the handler completed without exception
    // The test passes if no exception is thrown during envelope processing
  }

  @Test
  def testErrorCacheExpirationBasedEviction(): Unit = {
    // Create manager with small cache size for testing
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = 3)
    
    val requestContext = initializeRequestContextWithUserPrincipal()
    
    // Create 5 topics to exceed the cache size of 3
    val topicNames = (1 to 5).map(i => s"test-topic-$i")
    
    // Add errors for all 5 topics to the cache
    topicNames.zipWithIndex.foreach { case (topicName, idx) =>
      val topics = Map(
        topicName -> new CreatableTopic().setName(topicName).setNumPartitions(1).setReplicationFactor(1)
      )
      
      autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)
      
      val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
      Mockito.verify(brokerToController, Mockito.atLeastOnce()).sendRequest(
        any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
        argumentCaptor.capture())
      
      // Simulate error response for this topic
      val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
      val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
        .setName(topicName)
        .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
        .setErrorMessage(s"Topic '$topicName' already exists.")
      createTopicsResponseData.topics().add(topicResult)
      
      val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
      val header = new RequestHeader(ApiKeys.CREATE_TOPICS, 0, "client", 1)
      val clientResponse = new ClientResponse(header, null, null,
        0, 0, false, null, null, createTopicsResponse)
      
      argumentCaptor.getValue.onComplete(clientResponse)
      
      // Advance time slightly between additions to ensure different timestamps
      mockTime.sleep(10)
      
    }
    
    // With cache size of 3, topics 1 and 2 should have been evicted
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(topicNames.toSet, mockTime.milliseconds())
    
    // Only the last 3 topics should be in the cache (topics 3, 4, 5)
    assertEquals(3, cachedErrors.size, "Cache should contain only the most recent 3 entries")
    assertTrue(cachedErrors.contains("test-topic-3"), "test-topic-3 should be in cache")
    assertTrue(cachedErrors.contains("test-topic-4"), "test-topic-4 should be in cache")
    assertTrue(cachedErrors.contains("test-topic-5"), "test-topic-5 should be in cache")
    assertTrue(!cachedErrors.contains("test-topic-1"), "test-topic-1 should have been evicted")
    assertTrue(!cachedErrors.contains("test-topic-2"), "test-topic-2 should have been evicted")
  }

  @Test
  def testTopicsInBackoffAreNotRetried(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    // First attempt - trigger topic creation
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController, Mockito.times(1)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Simulate error response to cache the error
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val header = new RequestHeader(ApiKeys.CREATE_TOPICS, 0, "client", 1)
    val clientResponse = new ClientResponse(header, null, null,
      0, 0, false, null, null, createTopicsResponse)

    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify error is cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)

    // Second attempt - should NOT send request because topic is in back-off
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    // Verify still only one request was sent (not retried during back-off)
    Mockito.verify(brokerToController, Mockito.times(1)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  @Test
  def testTopicsOutOfBackoffCanBeRetried(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val shortTtlMs = 1000L

    // First attempt - trigger topic creation
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController, Mockito.times(1)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor.capture())

    // Simulate error response to cache the error
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val header = new RequestHeader(ApiKeys.CREATE_TOPICS, 0, "client", 1)
    val clientResponse = new ClientResponse(header, null, null,
      0, 0, false, null, null, createTopicsResponse)

    argumentCaptor.getValue.onComplete(clientResponse)

    // Verify error is cached
    val cachedErrors1 = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors1.size)

    // Advance time beyond TTL to exit back-off period
    mockTime.sleep(shortTtlMs + 100)

    // Verify error is expired
    val cachedErrors2 = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertTrue(cachedErrors2.isEmpty, "Error should be expired after TTL")

    // Second attempt - should send request because topic is out of back-off
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs)

    // Verify a second request was sent (retry allowed after back-off expires)
    Mockito.verify(brokerToController, Mockito.times(2)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  @Test
  def testInflightTopicsAreNotRetriedConcurrently(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    // First call - should send request and mark topic as in-flight
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    Mockito.verify(brokerToController, Mockito.times(1)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      any(classOf[ControllerRequestCompletionHandler]))

    // Second concurrent call - should NOT send request because topic is in-flight
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    // Verify still only one request was sent (concurrent request blocked)
    Mockito.verify(brokerToController, Mockito.times(1)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  @Test
  def testBackoffAndInflightInteraction(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "backoff-topic" -> new CreatableTopic().setName("backoff-topic").setNumPartitions(1).setReplicationFactor(1),
      "inflight-topic" -> new CreatableTopic().setName("inflight-topic").setNumPartitions(1).setReplicationFactor(1),
      "normal-topic" -> new CreatableTopic().setName("normal-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    // Create error for backoff-topic
    val backoffOnly = Map("backoff-topic" -> topics("backoff-topic"))
    autoTopicCreationManager.createStreamsInternalTopics(backoffOnly, requestContext, timeoutMs)

    val argumentCaptor1 = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    Mockito.verify(brokerToController, Mockito.times(1)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      argumentCaptor1.capture())

    // Simulate error response for backoff-topic
    val createTopicsResponseData = new org.apache.kafka.common.message.CreateTopicsResponseData()
    val topicResult = new org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult()
      .setName("backoff-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    val header = new RequestHeader(ApiKeys.CREATE_TOPICS, 0, "client", 1)
    val clientResponse = new ClientResponse(header, null, null,
      0, 0, false, null, null, createTopicsResponse)

    argumentCaptor1.getValue.onComplete(clientResponse)

    // Make inflight-topic in-flight (without completing the request)
    val inflightOnly = Map("inflight-topic" -> topics("inflight-topic"))
    autoTopicCreationManager.createStreamsInternalTopics(inflightOnly, requestContext, timeoutMs)

    Mockito.verify(brokerToController, Mockito.times(2)).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      any(classOf[ControllerRequestCompletionHandler]))

    // Now attempt to create all three topics together
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    val argumentCaptor2 = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[_ <: AbstractRequest]])
    // Total 3 requests: 1 for backoff-topic, 1 for inflight-topic, 1 for normal-topic only
    Mockito.verify(brokerToController, Mockito.times(3)).sendRequest(
      argumentCaptor2.capture(),
      any(classOf[ControllerRequestCompletionHandler]))

    // Verify that only normal-topic was included in the last request
    val lastRequest = argumentCaptor2.getValue.asInstanceOf[EnvelopeRequest.Builder]
      .build(ApiKeys.ENVELOPE.latestVersion())
    val forwardedRequestBuffer = lastRequest.requestData().duplicate()
    val requestHeader = RequestHeader.parse(forwardedRequestBuffer)
    val parsedRequest = CreateTopicsRequest.parse(new org.apache.kafka.common.protocol.ByteBufferAccessor(forwardedRequestBuffer),
      requestHeader.apiVersion())

    val topicNames = parsedRequest.data().topics().asScala.map(_.name()).toSet
    assertEquals(1, topicNames.size, "Only normal-topic should be created")
    assertTrue(topicNames.contains("normal-topic"), "normal-topic should be in the request")
    assertTrue(!topicNames.contains("backoff-topic"), "backoff-topic should be filtered (in back-off)")
    assertTrue(!topicNames.contains("inflight-topic"), "inflight-topic should be filtered (in-flight)")
  }
}
