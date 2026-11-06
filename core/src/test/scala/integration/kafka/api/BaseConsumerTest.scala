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

import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener, PartitionInfo, TopicPartition}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

/**
 * Integration tests for the consumer that cover basic usage as well as coordinator failure
 */
abstract class BaseConsumerTest extends AbstractConsumerTest {

  protected val tp = new TopicPartition(topic, 0)

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

    consumer.setRebalanceListener(listener)
    consumer.subscribe(java.util.List.of(topic))

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

  private class TestConsumerReassignmentListener extends RebalanceListener {
    var callsToAssigned = 0
    var callsToRevoked = 0

    def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition], rebalanceConsumer: RebalanceConsumer): Unit = {
      info("onPartitionsAssigned called.")
      callsToAssigned += 1
    }

    def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition], rebalanceConsumer: RebalanceConsumer): Unit = {
      info("onPartitionsRevoked called.")
      callsToRevoked += 1
    }
  }

  protected def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
                            tp: TopicPartition,
                            startingTimestamp: Long): Seq[ProducerRecord[Array[Byte], Array[Byte]]] = {
    val records = (0 until numRecords).map { i =>
      val timestamp = startingTimestamp + i.toLong
      val record = new ProducerRecord(tp.topic(), tp.partition(), timestamp, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
      record
    }
    producer.flush()

    records
  }

  protected def consumeAndVerifyRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                                        numRecords: Int,
                                        startingOffset: Int,
                                        startingTimestamp: Long): Unit = {
    val records = consumeRecords(consumer, numRecords)
    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(tp.topic, record.topic)
      assertEquals(tp.partition, record.partition)
      assertEquals(TimestampType.CREATE_TIME, record.timestampType)
      assertEquals(startingTimestamp + i, record.timestamp)
      assertEquals(offset.toLong, record.offset)
      assertEquals(s"key $i", new String(record.key))
      assertEquals(s"value $i", new String(record.value))
      // this is true only because K and V are byte arrays
      assertEquals(s"key $i".length, record.serializedKeySize)
      assertEquals(s"value $i".length, record.serializedValueSize)
    }
  }

  private def consumeRecords[K, V](consumer: Consumer[K, V],
                                   numRecords: Int): ArrayBuffer[ConsumerRecord[K, V]] = {
    val records = new ArrayBuffer[ConsumerRecord[K, V]]
    def pollAction(polledRecords: ConsumerRecords[K, V]): Boolean = {
      records ++= polledRecords.asScala
      records.size >= numRecords
    }
    TestUtils.pollRecordsUntilTrue(consumer, pollAction, waitTimeMs = 60000,
      msg = s"Timed out before consuming expected $numRecords records. " +
        s"The number consumed was ${records.size}.")
    records
  }

  protected def sendAndAwaitAsyncCommit[K, V](consumer: Consumer[K, V]): Unit = {

    class RetryCommitCallback extends OffsetCommitCallback {
      var isComplete = false
      var error: Option[Exception] = None

      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        exception match {
          case e: RetriableCommitFailedException =>
            consumer.commitAsync(this)
          case e =>
            isComplete = true
            error = Option(e)
        }
      }
    }

    val commitCallback = new RetryCommitCallback

    consumer.commitAsync(commitCallback)
    TestUtils.pollUntilTrue(consumer, () => commitCallback.isComplete,
      "Failed to observe commit callback before timeout", waitTimeMs = 10000)

    assertEquals(None, commitCallback.error)
  }

  private def awaitRebalance(consumer: Consumer[_, _], rebalanceListener: TestConsumerReassignmentListener): Unit = {
    val numReassignments = rebalanceListener.callsToAssigned
    TestUtils.pollUntilTrue(consumer, () => rebalanceListener.callsToAssigned > numReassignments,
      "Timed out before expected rebalance completed")
  }

  private def ensureNoRebalance(consumer: Consumer[_, _], rebalanceListener: TestConsumerReassignmentListener): Unit = {
    // The best way to verify that the current membership is still active is to commit offsets.
    // This would fail if the group had rebalanced.
    val initialRevokeCalls = rebalanceListener.callsToRevoked
    sendAndAwaitAsyncCommit(consumer)
    assertEquals(initialRevokeCalls, rebalanceListener.callsToRevoked)
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
