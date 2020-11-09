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

import java.util.Properties

import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createBroker
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME}
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentMatchers, Mockito}

import scala.collection.{Map, Seq}

class AutoTopicCreationManagerTest {

  private val requestTimeout = 100
  private var config: KafkaConfig = _
  private val metadataCache = Mockito.mock(classOf[MetadataCache])
  private val brokerToController = Mockito.mock(classOf[BrokerToControllerChannelManager])
  private val adminManager = Mockito.mock(classOf[ZkAdminManager])
  private val controller = Mockito.mock(classOf[KafkaController])
  private val groupCoordinator = Mockito.mock(classOf[GroupCoordinator])
  private val transactionCoordinator = Mockito.mock(classOf[TransactionCoordinator])
  private var autoTopicCreationManager: AutoTopicCreationManager = _

  private val internalTopicPartitions = 2
  private val internalTopicReplicationFactor: Short = 2

  @BeforeEach
  def setup(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost")
    props.setProperty(KafkaConfig.RequestTimeoutMsProp, requestTimeout.toString)

    props.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, internalTopicPartitions.toString)
    props.setProperty(KafkaConfig.TransactionsTopicReplicationFactorProp, internalTopicPartitions.toString)

    props.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, internalTopicReplicationFactor.toString)
    props.setProperty(KafkaConfig.TransactionsTopicPartitionsProp, internalTopicReplicationFactor.toString)

    config = KafkaConfig.fromProps(props)
    val aliveBrokers = Seq(createBroker(0, "host0", 0), createBroker(1, "host1", 1))

    Mockito.reset(metadataCache, controller, brokerToController, groupCoordinator, transactionCoordinator)

    Mockito.when(metadataCache.getAliveBrokers).thenReturn(aliveBrokers)
  }

  @Test
  def testCreateOffsetTopic(): Unit = {
    Mockito.when(groupCoordinator.offsetsTopicConfigs).thenReturn(new Properties)
    testCreateTopic(GROUP_METADATA_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor)
  }

  @Test
  def testCreateTxnTopic(): Unit = {
    Mockito.when(transactionCoordinator.transactionTopicConfigs).thenReturn(new Properties)
    testCreateTopic(TRANSACTION_STATE_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor)
  }

  @Test
  def testCreateNonInternalTopic(): Unit = {
    testCreateTopic("topic", false)
  }

  private def testCreateTopic(topicName: String,
                              isInternal: Boolean,
                              numPartitions: Int = 1,
                              replicationFactor: Short = 1): Unit = {
    autoTopicCreationManager = new AutoTopicCreationManagerImpl(
      config,
      metadataCache,
      Some(brokerToController),
      adminManager,
      controller,
      groupCoordinator,
      transactionCoordinator)

    val topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection
    topicsCollection.add(getNewTopic(topicName, numPartitions, replicationFactor))
    val requestBody = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTopics(topicsCollection)
        .setTimeoutMs(requestTimeout))

    Mockito.when(controller.isActive).thenReturn(false)

    // Calling twice with the same topic will only trigger one forwarding.
    createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal)
    createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal)

    Mockito.verify(brokerToController).sendRequest(
      ArgumentMatchers.eq(requestBody),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  @Test
  def testCreateTopicsWithForwardingDisabled(): Unit = {
    autoTopicCreationManager = new AutoTopicCreationManagerImpl(
      config,
      metadataCache,
      None,
      adminManager,
      controller,
      groupCoordinator,
      transactionCoordinator)

    val topicName = "topic"

    Mockito.when(controller.isActive).thenReturn(false)

    createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, false)

    Mockito.verify(adminManager).createTopics(
      ArgumentMatchers.eq(requestTimeout),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Map(topicName -> getNewTopic(topicName))),
      ArgumentMatchers.eq(Map.empty),
      any(classOf[ControllerMutationQuota]),
      any(classOf[Map[String, ApiError] => Unit]))
  }

  @Test
  def testNotEnoughLiveBrokers(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost")
    props.setProperty(KafkaConfig.DefaultReplicationFactorProp, 3.toString)
    config = KafkaConfig.fromProps(props)

    autoTopicCreationManager = new AutoTopicCreationManagerImpl(
      config,
      metadataCache,
      Some(brokerToController),
      adminManager,
      controller,
      groupCoordinator,
      transactionCoordinator)

    val topicName = "topic"

    Mockito.when(controller.isActive).thenReturn(false)

    createTopicAndVerifyResult(Errors.INVALID_REPLICATION_FACTOR, topicName, false)

    Mockito.verify(brokerToController, Mockito.never()).sendRequest(
      any(),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  private def createTopicAndVerifyResult(error: Errors,
                                         topicName: String,
                                         isInternal: Boolean): Unit = {
    val topicResponses = autoTopicCreationManager.createTopics(
      Set(topicName), UnboundedControllerMutationQuota)

    val expectedResponses = Seq(new MetadataResponseTopic()
      .setErrorCode(error.code())
      .setIsInternal(isInternal)
      .setName(topicName))

    assertEquals(expectedResponses, topicResponses)
  }

  private def getNewTopic(topicName: String, numPartitions: Int = 1, replicationFactor: Short = 1): CreatableTopic = {
    new CreatableTopic()
      .setName(topicName)
      .setNumPartitions(numPartitions)
      .setReplicationFactor(replicationFactor)
  }
}
