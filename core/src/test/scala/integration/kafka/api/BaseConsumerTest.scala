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
package kafka.api

import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, GroupProtocol}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener, PartitionInfo}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.collection.Seq

/**
 * Integration tests for the consumer that cover basic usage as well as coordinator failure
 */
abstract class BaseConsumerTest extends AbstractConsumerTest {

  private var currentTestName: String = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    currentTestName = testInfo.getTestMethod.get().getName
    super.setUp(testInfo)
  }

  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)

    if (currentTestName != null && currentTestName.equals("testCoordinatorFailover")) {
      // Enable controlled shutdown to allow the broker to notify the controller before shutting down.
      // This speeds up the test by triggering an immediate failover instead of waiting for the
      // broker session timeout (default: 9s) to expire.
      properties.setProperty(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "true")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSimpleConsumption(groupProtocol: String): Unit = {
    val numRecords = 10000
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment.size)
    consumer.assign(java.util.List.of(tp))
    assertEquals(1, consumer.assignment.size)

    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    // check async commit callbacks
    sendAndAwaitAsyncCommit(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testClusterResourceListener(groupProtocol: String): Unit = {
    val numRecords = 100
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[BaseConsumerTest.TestClusterResourceListenerSerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[BaseConsumerTest.TestClusterResourceListenerSerializer])

    val producer: KafkaProducer[Array[Byte], Array[Byte]] = createProducer(keySerializer = null, valueSerializer = null, producerProps)
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[BaseConsumerTest.TestClusterResourceListenerDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[BaseConsumerTest.TestClusterResourceListenerDeserializer])
    val consumer: Consumer[Array[Byte], Array[Byte]] = createConsumer(keyDeserializer = null, valueDeserializer = null, consumerProps)
    consumer.subscribe(java.util.List.of(tp.topic()))
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)
    assertNotEquals(0, BaseConsumerTest.updateProducerCount.get())
    assertNotEquals(0, BaseConsumerTest.updateConsumerCount.get())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCoordinatorFailover(groupProtocol: String): Unit = {
    val listener = new TestConsumerReassignmentListener()
    if (groupProtocol.equalsIgnoreCase(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "5001")
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    }
    // Use higher poll timeout to avoid consumer leaving the group due to timeout
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "15000")
    val consumer = createConsumer()

    consumer.subscribe(java.util.List.of(topic), listener)

    // the initial subscription should cause a callback execution
    awaitRebalance(consumer, listener)
    assertEquals(1, listener.callsToAssigned)

    // get metadata for the topic
    var parts: Seq[PartitionInfo] = null
    while (parts == null)
      parts = consumer.partitionsFor(Topic.GROUP_METADATA_TOPIC_NAME).asScala
    assertEquals(1, parts.size)
    assertNotNull(parts.head.leader())

    // shutdown the coordinator
    val coordinator = parts.head.leader().id()
    this.brokers(coordinator).shutdown()

    // the failover should not cause a rebalance
    ensureNoRebalance(consumer, listener)
  }
}

object BaseConsumerTest {

  val updateProducerCount = new AtomicInteger()
  val updateConsumerCount = new AtomicInteger()

  class TestClusterResourceListenerSerializer extends Serializer[Array[Byte]] with ClusterResourceListener {

    override def onUpdate(clusterResource: ClusterResource): Unit = updateProducerCount.incrementAndGet()

    override def serialize(topic: String, data: Array[Byte]): Array[Byte] = data
  }

  class TestClusterResourceListenerDeserializer extends Deserializer[Array[Byte]] with ClusterResourceListener {

    override def onUpdate(clusterResource: ClusterResource): Unit = updateConsumerCount.incrementAndGet()
    override def deserialize(topic: String, data: Array[Byte]): Array[Byte] = data
  }
}
