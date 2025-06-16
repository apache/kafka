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
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.{SecurityUtils, Utils}
import org.apache.kafka.coordinator.group.{GroupCoordinator, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorConfig}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.never
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import scala.collection.{Map, Seq}

class AutoTopicCreationManagerTest {

  private val requestTimeout = 100
  private var config: KafkaConfig = _
  private val metadataCache = Mockito.mock(classOf[MetadataCache])
  private val brokerToController = Mockito.mock(classOf[NodeToControllerChannelManager])
  private val groupCoordinator = Mockito.mock(classOf[GroupCoordinator])
  private val transactionCoordinator = Mockito.mock(classOf[TransactionCoordinator])
  private val shareCoordinator = Mockito.mock(classOf[ShareCoordinator])
  private var autoTopicCreationManager: AutoTopicCreationManager = _

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
      shareCoordinator)

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
      Set(topicName), UnboundedControllerMutationQuota, Some(requestContext))

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
      Set(topicName), UnboundedControllerMutationQuota, Some(requestContext)))
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
      Set(topicName), UnboundedControllerMutationQuota, Some(requestContext))
    autoTopicCreationManager.createTopics(
      Set(topicName), UnboundedControllerMutationQuota, Some(requestContext))

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
      Set(topicName), UnboundedControllerMutationQuota, Some(requestContext))
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
      shareCoordinator)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext)

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
      shareCoordinator)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext)

    Mockito.verify(brokerToController, never()).sendRequest(
      any(classOf[AbstractRequest.Builder[_ <: AbstractRequest]]),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  @Test
  def testCreateStreamsInternalTopicsWithDefaultConfig(): Unit = {
    val topics = Map(
      "stream-topic-1" -> new CreatableTopic().setName("stream-topic-1").setNumPartitions(-1).setReplicationFactor(-1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()

    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      brokerToController,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[_ <: AbstractRequest]])
    Mockito.verify(brokerToController).sendRequest(
      argumentCaptor.capture(),
      any(classOf[ControllerRequestCompletionHandler]))

    val capturedRequest = argumentCaptor.getValue.asInstanceOf[EnvelopeRequest.Builder].build(ApiKeys.ENVELOPE.latestVersion())

    val requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, ApiKeys.CREATE_TOPICS.latestVersion(), "clientId", 0)
    val topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection
    topicsCollection.add(getNewTopic("stream-topic-1", config.numPartitions, config.defaultReplicationFactor.toShort))
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
      shareCoordinator)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext)

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
      shareCoordinator)

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
      Set(topicName), UnboundedControllerMutationQuota, metadataContext)

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
}
