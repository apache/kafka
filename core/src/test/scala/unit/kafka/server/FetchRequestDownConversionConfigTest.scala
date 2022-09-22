/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util.Properties

import kafka.log.LogConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class FetchRequestDownConversionConfigTest extends BaseFetchRequestTest {
  override def brokerCount: Int = 2

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    initProducer()
  }

  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.LogMessageDownConversionEnableProp, "false")
  }

  /**
   * Tests that fetch request that require down-conversion returns with an error response when down-conversion is disabled on broker.
   */
  @Test
  def testV1FetchWithDownConversionDisabled(): Unit = {
    testWithDownConversionDisabled(1, Errors.UNSUPPORTED_VERSION)
  }

  /**
   * Tests that "message.downconversion.enable" has no effect when down-conversion is not required on last version before topic IDs.
   */
  @Test
  def testV12WithDownConversionDisabled(): Unit = {
    testWithDownConversionDisabled(12, Errors.NONE)
  }

  /**
   * Tests that "message.downconversion.enable" has no effect when down-conversion is not required.
   */
  @Test
  def testLatestFetchWithDownConversionDisabled(): Unit = {
    testWithDownConversionDisabled(ApiKeys.FETCH.latestVersion, Errors.NONE)
  }

  private def testWithDownConversionDisabled(
    version: Short,
    expectedError: Errors
  ): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaIds = brokers.map(_.config.brokerId)
    val leaderId = replicaIds.head

    val admin = createAdminClient()

    val topicId = TestUtils.createTopicWithAdminRaw(
      admin,
      tp.topic,
      replicaAssignment = Map(0 -> replicaIds)
    )
    val topicIdMap = Map(tp.topic -> topicId)

    producer.send(new ProducerRecord(tp.topic, "key", "value")).get()

    val fetchResponseData = sendFetch(
      leaderId,
      Seq(tp),
      topicIdMap,
      fetchVersion = version,
      replicaIdOpt = None
    )

    assertEquals(expectedError, Errors.forCode(fetchResponseData.get(tp).errorCode))
  }

  /**
   * Tests that "message.downconversion.enable" can be set at topic level, and its configuration is obeyed for client
   * fetch requests.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testV1FetchFromConsumer(quorum: String): Unit = {
    testV1Fetch(isFollowerFetch = false)
  }

  /**
   * Tests that "message.downconversion.enable" has no effect on fetch requests from replicas.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testV1FetchFromReplica(quorum: String): Unit = {
    testV1Fetch(isFollowerFetch = true)
  }

  def testV1Fetch(isFollowerFetch: Boolean): Unit = {
    val topicWithDownConversionEnabled = "foo"
    val topicWithDownConversionDisabled = "bar"
    val replicaIds = brokers.map(_.config.brokerId)
    val leaderId = replicaIds.head
    val followerId = replicaIds.last

    val admin = createAdminClient()

    val topicWithDownConversionDisabledId = TestUtils.createTopicWithAdminRaw(
      admin,
      topicWithDownConversionDisabled,
      replicaAssignment = Map(0 -> replicaIds)
    )

    val topicConfig = new Properties
    topicConfig.put(LogConfig.MessageDownConversionEnableProp, "true")
    val topicWithDownConversionEnabledId = TestUtils.createTopicWithAdminRaw(
      admin,
      topicWithDownConversionEnabled,
      replicaAssignment = Map(0 -> replicaIds),
      topicConfig = topicConfig
    )

    val partitionWithDownConversionEnabled = new TopicPartition(topicWithDownConversionEnabled, 0)
    val partitionWithDownConversionDisabled = new TopicPartition(topicWithDownConversionDisabled, 0)

    val allTopicPartitions = Seq(
      partitionWithDownConversionEnabled,
      partitionWithDownConversionDisabled
    )

    allTopicPartitions.foreach { tp =>
      producer.send(new ProducerRecord(tp.topic, "key", "value")).get()
    }

    val topicIdMap = Map(
      topicWithDownConversionEnabled -> topicWithDownConversionEnabledId,
      topicWithDownConversionDisabled -> topicWithDownConversionDisabledId
    )

    val fetchResponseData = sendFetch(
      leaderId,
      allTopicPartitions,
      topicIdMap,
      fetchVersion = 1,
      replicaIdOpt = if (isFollowerFetch) Some(followerId) else None
    )

    def error(tp: TopicPartition): Errors = {
      Errors.forCode(fetchResponseData.get(tp).errorCode)
    }

    assertEquals(Errors.NONE, error(partitionWithDownConversionEnabled))
    if (isFollowerFetch) {
      assertEquals(Errors.NONE, error(partitionWithDownConversionDisabled))
    } else {
      assertEquals(Errors.UNSUPPORTED_VERSION, error(partitionWithDownConversionDisabled))
    }
  }
}
