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
import java.util
import java.util.concurrent.CompletableFuture
import java.util.{Optional, Properties}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.utils.TestUtils
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME}
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicConfig, CreatableTopicConfigCollection}
import org.apache.kafka.common.message.CreateTopicsResponseData
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.{SecurityUtils, Utils}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.coordinator.group.{GroupCoordinator, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorConfig}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.quota.ControllerMutationQuota
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.{ArgumentMatchers, Mockito}

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer

/**
 * Test implementation of TopicCreator that tracks method calls and allows configuring responses.
 */
class TestTopicCreator extends TopicCreator {
  private val withPrincipalCalls = ListBuffer[(RequestContext, CreateTopicsRequest.Builder)]()
  private val withoutPrincipalCalls = ListBuffer[CreateTopicsRequest.Builder]()
  private var withPrincipalResponse: CompletableFuture[CreateTopicsResponse] = _
  private var withoutPrincipalResponse: CompletableFuture[CreateTopicsResponse] = _

  override def createTopicWithPrincipal(
    requestContext: RequestContext,
    request: CreateTopicsRequest.Builder
  ): CompletableFuture[CreateTopicsResponse] = {
    withPrincipalCalls += ((requestContext, request))
    if (withPrincipalResponse != null) withPrincipalResponse else CompletableFuture.completedFuture(null)
  }

  override def createTopicWithoutPrincipal(
    request: CreateTopicsRequest.Builder
  ): CompletableFuture[CreateTopicsResponse] = {
    withoutPrincipalCalls += request
    if (withoutPrincipalResponse != null) withoutPrincipalResponse else CompletableFuture.completedFuture(null)
  }

  def setResponseForWithPrincipal(response: CreateTopicsResponse): Unit = {
    withPrincipalResponse = CompletableFuture.completedFuture(response)
  }

  def setResponseForWithoutPrincipal(response: CreateTopicsResponse): Unit = {
    withoutPrincipalResponse = CompletableFuture.completedFuture(response)
  }

  def setFutureForWithPrincipal(future: CompletableFuture[CreateTopicsResponse]): Unit = {
    withPrincipalResponse = future
  }

  def setFutureForWithoutPrincipal(future: CompletableFuture[CreateTopicsResponse]): Unit = {
    withoutPrincipalResponse = future
  }

  def getWithPrincipalCalls: List[(RequestContext, CreateTopicsRequest.Builder)] = withPrincipalCalls.toList
  def getWithoutPrincipalCalls: List[CreateTopicsRequest.Builder] = withoutPrincipalCalls.toList

  def withPrincipalCallCount: Int = withPrincipalCalls.size
  def withoutPrincipalCallCount: Int = withoutPrincipalCalls.size

  def reset(): Unit = {
    withPrincipalCalls.clear()
    withoutPrincipalCalls.clear()
    withPrincipalResponse = null
    withoutPrincipalResponse = null
  }
}

class AutoTopicCreationManagerTest {

  private val requestTimeout = 100
  private val testCacheCapacity = 3
  private var config: KafkaConfig = _
  private val metadataCache = Mockito.mock(classOf[MetadataCache])
  private val topicCreator = new TestTopicCreator()
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

    config = KafkaConfig.fromProps(props)
    val aliveBrokers = util.List.of(new Node(0, "host0", 0), new Node(1, "host1", 1))

    Mockito.when(metadataCache.getAliveBrokerNodes(ArgumentMatchers.any(classOf[ListenerName]))).thenReturn(aliveBrokers)
    topicCreator.reset()
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
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity)

    // Set up the topicCreator to return a successful response
    val createTopicsResponseData = new CreateTopicsResponseData()
    val topicResult = new CreatableTopicResult()
      .setName(topicName)
      .setErrorCode(Errors.NONE.code())
    createTopicsResponseData.topics().add(topicResult)
    val response = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithoutPrincipal(response)

    // First call to create topic - should trigger the topic creator
    createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal)

    assertEquals(1, topicCreator.withoutPrincipalCallCount, "Should have called createTopicWithoutPrincipal once")

    // Reset the topicCreator to verify the second call
    topicCreator.reset()
    topicCreator.setResponseForWithoutPrincipal(response)

    // Second call - should also trigger topicCreator because inflight is cleared after first call completes
    createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal)

    assertEquals(1, topicCreator.withoutPrincipalCallCount, "Should have called createTopicWithoutPrincipal once more")

    // Verify the request builder matches expected values
    val capturedRequest = topicCreator.getWithoutPrincipalCalls.head.build()
    assertEquals(requestTimeout, capturedRequest.data().timeoutMs())
    assertEquals(1, capturedRequest.data().topics().size())

    // Validate request
    val topic = capturedRequest.data().topics().iterator().next()
    assertEquals(topicName, topic.name())
    assertEquals(numPartitions, topic.numPartitions())
    assertEquals(replicationFactor, topic.replicationFactor())
  }

  @Test
  def testTopicCreationWithMetadataContext(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity)

    val topicName = "topic"
    val userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user")
    val principalSerde = new KafkaPrincipalSerde {
      override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
      override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
    }

    val requestContext = initializeRequestContext(userPrincipal, Optional.of(principalSerde))

    val createTopicsResponseData = new CreateTopicsResponseData()
    val topicResult = new CreatableTopicResult()
      .setName(topicName)
      .setErrorCode(Errors.NONE.code())
    createTopicsResponseData.topics().add(topicResult)
    val response = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(response)

    autoTopicCreationManager.createTopics(
      Set(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, Some(requestContext))

    assertEquals(1, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal once")
    val calls = topicCreator.getWithPrincipalCalls
    assertEquals(requestContext, calls.head._1)

    val capturedRequest = calls.head._2.build()
    assertEquals(1, capturedRequest.data().topics().size())
    assertEquals(topicName, capturedRequest.data().topics().iterator().next().name())
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
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity
    )

    val createTopicsResponseData = new CreateTopicsResponseData()
    createTopicsResponseData.topics().add(
      new CreatableTopicResult()
        .setName("stream-topic-1")
        .setErrorCode(Errors.NONE.code()))
    createTopicsResponseData.topics().add(
      new CreatableTopicResult()
        .setName("stream-topic-2")
        .setErrorCode(Errors.NONE.code()))
    val response = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(response)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    assertEquals(1, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal once")
    val calls = topicCreator.getWithPrincipalCalls
    assertEquals(requestContext, calls.head._1)

    val capturedRequest = calls.head._2.build()
    assertEquals(requestTimeout, capturedRequest.data().timeoutMs())
    assertEquals(2, capturedRequest.data().topics().size())
    val topicNames = capturedRequest.data().topics().asScala.map(_.name()).toSet
    assertTrue(topicNames.contains("stream-topic-1"))
    assertTrue(topicNames.contains("stream-topic-2"))
  }

  @Test
  def testCreateStreamsInternalTopicsWithEmptyTopics(): Unit = {
    val topics = Map.empty[String, CreatableTopic]
    val requestContext = initializeRequestContextWithUserPrincipal()

    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    assertEquals(0, topicCreator.withPrincipalCallCount, "Should not have called createTopicWithPrincipal")
  }

  @Test
  def testCreateStreamsInternalTopicsPassesRequestContext(): Unit = {
    val topics = Map(
      "stream-topic-1" -> new CreatableTopic().setName("stream-topic-1").setNumPartitions(-1).setReplicationFactor(-1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()

    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity
    )

    val createTopicsResponseData = new CreateTopicsResponseData()
    createTopicsResponseData.topics().add(
      new CreatableTopicResult()
        .setName("stream-topic-1")
        .setErrorCode(Errors.NONE.code()))
    val response = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(response)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

    assertEquals(1, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal once")
    val calls = topicCreator.getWithPrincipalCalls
    assertEquals(requestContext, calls.head._1)
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

  @Test
  def testTopicCreationErrorCaching(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity
    )

    val topics = Map(
      "test-topic-1" -> new CreatableTopic().setName("test-topic-1").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()

    // Simulate a CreateTopicsResponse with errors
    val createTopicsResponseData = new CreateTopicsResponseData()
    val topicResult = new CreatableTopicResult()
      .setName("test-topic-1")
      .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
      .setErrorMessage("Topic 'test-topic-1' already exists.")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(createTopicsResponse)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

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
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity
    )

    val topics = Map(
      "success-topic" -> new CreatableTopic().setName("success-topic").setNumPartitions(1).setReplicationFactor(1),
      "failed-topic" -> new CreatableTopic().setName("failed-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()

    // Simulate mixed response - one success, one failure
    val createTopicsResponseData = new CreateTopicsResponseData()
    createTopicsResponseData.topics().add(
      new CreatableTopicResult()
        .setName("success-topic")
        .setErrorCode(Errors.NONE.code())
    )
    createTopicsResponseData.topics().add(
      new CreatableTopicResult()
        .setName("failed-topic")
        .setErrorCode(Errors.POLICY_VIOLATION.code())
        .setErrorMessage("Policy violation")
    )

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(createTopicsResponse)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)

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
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity
    )

    // First cache an error by simulating topic creation failure
    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val shortTtlMs = 1000L // Use 1 second TTL for faster testing

    // Simulate a CreateTopicsResponse with error
    val createTopicsResponseData = new CreateTopicsResponseData()
    val topicResult = new CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(createTopicsResponse)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs)

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
  def testErrorCacheExpirationBasedEviction(): Unit = {
    // Create manager with small cache size for testing
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = 3)
    
    val requestContext = initializeRequestContextWithUserPrincipal()
    
    // Create 5 topics to exceed the cache size of 3
    val topicNames = (1 to 5).map(i => s"test-topic-$i")
    
    // Add errors for all 5 topics to the cache
    topicNames.zipWithIndex.foreach { case (topicName, idx) =>
      val topics = Map(
        topicName -> new CreatableTopic().setName(topicName).setNumPartitions(1).setReplicationFactor(1)
      )
      
      // Simulate error response for this topic
      val createTopicsResponseData = new CreateTopicsResponseData()
      val topicResult = new CreatableTopicResult()
        .setName(topicName)
        .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
        .setErrorMessage(s"Topic '$topicName' already exists.")
      createTopicsResponseData.topics().add(topicResult)
      
      val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
      topicCreator.setResponseForWithPrincipal(createTopicsResponse)

      autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, config.groupCoordinatorConfig.streamsGroupHeartbeatIntervalMs() * 2)
      
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
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    // Simulate error response to cache the error
    val createTopicsResponseData = new CreateTopicsResponseData()
    val topicResult = new CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(createTopicsResponse)

    // First attempt - trigger topic creation
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    // Verify error is cached
    val cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(Set("test-topic"), mockTime.milliseconds())
    assertEquals(1, cachedErrors.size)

    // Second attempt - should NOT send request because topic is in back-off
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    // Verify still only one request was sent (not retried during back-off)
    assertEquals(1, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal once")
  }

  @Test
  def testTopicsOutOfBackoffCanBeRetried(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val shortTtlMs = 1000L

    // Simulate error response to cache the error
    val createTopicsResponseData = new CreateTopicsResponseData()
    val topicResult = new CreatableTopicResult()
      .setName("test-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    createTopicsResponseData.topics().add(topicResult)

    val createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData)
    topicCreator.setResponseForWithPrincipal(createTopicsResponse)

    // First attempt - trigger topic creation
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs)

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
    assertEquals(2, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal twice")
  }

  @Test
  def testInflightTopicsAreNotRetriedConcurrently(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "test-topic" -> new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    // Use a future that doesn't complete immediately to simulate in-flight state
    val future = new CompletableFuture[CreateTopicsResponse]()
    topicCreator.setFutureForWithPrincipal(future)

    // First call - should send request and mark topic as in-flight
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    assertEquals(1, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal once")

    // Second concurrent call - should NOT send request because topic is in-flight
    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    // Verify still only one request was sent (concurrent request blocked)
    assertEquals(1, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal once")
  }

  @Test
  def testBackoffAndInflightInteraction(): Unit = {
    autoTopicCreationManager = new DefaultAutoTopicCreationManager(
      config,
      groupCoordinator,
      transactionCoordinator,
      shareCoordinator,
      mockTime,
      topicCreator,
      topicErrorCacheCapacity = testCacheCapacity)

    val topics = Map(
      "backoff-topic" -> new CreatableTopic().setName("backoff-topic").setNumPartitions(1).setReplicationFactor(1),
      "inflight-topic" -> new CreatableTopic().setName("inflight-topic").setNumPartitions(1).setReplicationFactor(1),
      "normal-topic" -> new CreatableTopic().setName("normal-topic").setNumPartitions(1).setReplicationFactor(1)
    )
    val requestContext = initializeRequestContextWithUserPrincipal()
    val timeoutMs = 5000L

    // Simulate error response for backoff-topic
    val backoffResponseData = new CreateTopicsResponseData()
    val backoffResult = new CreatableTopicResult()
      .setName("backoff-topic")
      .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
      .setErrorMessage("Invalid replication factor")
    backoffResponseData.topics().add(backoffResult)
    val backoffResponse = new CreateTopicsResponse(backoffResponseData)
    topicCreator.setResponseForWithPrincipal(backoffResponse)

    // Create error for backoff-topic
    val backoffOnly = Map("backoff-topic" -> topics("backoff-topic"))
    autoTopicCreationManager.createStreamsInternalTopics(backoffOnly, requestContext, timeoutMs)

    // Make inflight-topic in-flight (without completing the request)
    val inflightFuture = new CompletableFuture[CreateTopicsResponse]()
    topicCreator.setFutureForWithPrincipal(inflightFuture)

    val inflightOnly = Map("inflight-topic" -> topics("inflight-topic"))
    autoTopicCreationManager.createStreamsInternalTopics(inflightOnly, requestContext, timeoutMs)

    // Now attempt to create all three topics together
    val normalResponseData = new CreateTopicsResponseData()
    val normalResult = new CreatableTopicResult()
      .setName("normal-topic")
      .setErrorCode(Errors.NONE.code())
    normalResponseData.topics().add(normalResult)
    val normalResponse = new CreateTopicsResponse(normalResponseData)
    topicCreator.setResponseForWithPrincipal(normalResponse)

    autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs)

    // Total 3 requests: 1 for backoff-topic, 1 for inflight-topic, 1 for normal-topic only
    assertEquals(3, topicCreator.withPrincipalCallCount, "Should have called createTopicWithPrincipal 3 times")

    // Verify that only normal-topic was included in the last request
    val calls = topicCreator.getWithPrincipalCalls
    val lastRequest = calls(2)._2.build()
    val topicNames = lastRequest.data().topics().asScala.map(_.name()).toSet
    assertEquals(1, topicNames.size, "Only normal-topic should be created")
    assertTrue(topicNames.contains("normal-topic"), "normal-topic should be in the request")
    assertTrue(!topicNames.contains("backoff-topic"), "backoff-topic should be filtered (in back-off)")
    assertTrue(!topicNames.contains("inflight-topic"), "inflight-topic should be filtered (in-flight)")
  }
}
