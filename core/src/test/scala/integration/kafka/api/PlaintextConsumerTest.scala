/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import kafka.api.BaseConsumerTest.{DeserializerImpl, SerializerImpl}

import java.time.Duration
import java.util
import java.util.Arrays.asList
import java.util.{Collections, Locale, Optional, Properties}
import kafka.server.KafkaBroker
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.{NewPartitions, NewTopic}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{InterruptException, InvalidGroupIdException, InvalidTopicException, TimeoutException, WakeupException}
import org.apache.kafka.common.record.{CompressionType, TimestampType}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.test.api.Flaky
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.server.quota.QuotaType
import org.apache.kafka.test.{MockConsumerInterceptor, MockProducerInterceptor}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit}
import scala.jdk.CollectionConverters._

@Timeout(600)
class PlaintextConsumerTest extends BaseConsumerTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testHeaders(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 1
    val record = new ProducerRecord(tp.topic, tp.partition, null, "key".getBytes, "value".getBytes)

    record.headers().add("headerKey", "headerValue".getBytes)

    val producer = createProducer()
    producer.send(record)

    val consumer = createConsumer()
    assertEquals(0, consumer.assignment.size)
    consumer.assign(List(tp).asJava)
    assertEquals(1, consumer.assignment.size)

    consumer.seek(tp, 0)
    val records = consumeRecords(consumer = consumer, numRecords = numRecords)

    assertEquals(numRecords, records.size)

    for (i <- 0 until numRecords) {
      val record = records(i)
      val header = record.headers().lastHeader("headerKey")
      assertEquals("headerValue", if (header == null) null else new String(header.value()))
    }
  }

  private def testHeadersSerializeDeserialize(serializer: Serializer[Array[Byte]], deserializer: Deserializer[Array[Byte]]): Unit = {
    val numRecords = 1
    val record = new ProducerRecord(tp.topic, tp.partition, null, "key".getBytes, "value".getBytes)

    val producer = createProducer(
      keySerializer = new ByteArraySerializer,
      valueSerializer = serializer)
    producer.send(record)

    val consumer = createConsumer(
      keyDeserializer = new ByteArrayDeserializer,
      valueDeserializer = deserializer)
    assertEquals(0, consumer.assignment.size)
    consumer.assign(List(tp).asJava)
    assertEquals(1, consumer.assignment.size)

    consumer.seek(tp, 0)
    val records = consumeRecords(consumer = consumer, numRecords = numRecords)

    assertEquals(numRecords, records.size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testHeadersSerializerDeserializer(quorum: String, groupProtocol: String): Unit = {
    val extendedSerializer = new SerializerImpl

    val extendedDeserializer = new DeserializerImpl

    testHeadersSerializeDeserialize(extendedSerializer, extendedDeserializer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testAutoOffsetReset(quorum: String, groupProtocol: String): Unit = {
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 1, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testGroupConsumption(quorum: String, groupProtocol: String): Unit = {
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 10, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPartitionsFor(quorum: String, groupProtocol: String): Unit = {
    val numParts = 2
    createTopic("part-test", numParts)
    val consumer = createConsumer()
    val parts = consumer.partitionsFor("part-test")
    assertNotNull(parts)
    assertEquals(2, parts.size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPartitionsForAutoCreate(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    // First call would create the topic
    consumer.partitionsFor("non-exist-topic")
    TestUtils.waitUntilTrue(() => {
      !consumer.partitionsFor("non-exist-topic").isEmpty
    }, s"Timed out while awaiting non empty partitions.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPartitionsForInvalidTopic(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    assertThrows(classOf[InvalidTopicException], () => consumer.partitionsFor(";3# ads,{234"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSeek(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    val totalRecords = 50L
    val mid = totalRecords / 2

    // Test seek non-compressed message
    val producer = createProducer()
    val startingTimestamp = 0
    sendRecords(producer, totalRecords.toInt, tp, startingTimestamp = startingTimestamp)
    consumer.assign(List(tp).asJava)

    consumer.seekToEnd(List(tp).asJava)
    assertEquals(totalRecords, consumer.position(tp))
    assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty)

    consumer.seekToBeginning(List(tp).asJava)
    assertEquals(0L, consumer.position(tp))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = 0, startingTimestamp = startingTimestamp)

    consumer.seek(tp, mid)
    assertEquals(mid, consumer.position(tp))

    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = mid.toInt, startingKeyAndValueIndex = mid.toInt,
      startingTimestamp = mid)

    // Test seek compressed message
    sendCompressedMessages(totalRecords.toInt, tp2)
    consumer.assign(List(tp2).asJava)

    consumer.seekToEnd(List(tp2).asJava)
    assertEquals(totalRecords, consumer.position(tp2))
    assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty)

    consumer.seekToBeginning(List(tp2).asJava)
    assertEquals(0L, consumer.position(tp2))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = 0, tp = tp2)

    consumer.seek(tp2, mid)
    assertEquals(mid, consumer.position(tp2))
    consumeAndVerifyRecords(consumer, numRecords = 1, startingOffset = mid.toInt, startingKeyAndValueIndex = mid.toInt,
      startingTimestamp = mid, tp = tp2)
  }

  private def sendCompressedMessages(numRecords: Int, tp: TopicPartition): Unit = {
    val producerProps = new Properties()
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name)
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, Int.MaxValue.toString)
    val producer = createProducer(configOverrides = producerProps)
    (0 until numRecords).foreach { i =>
      producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, s"value $i".getBytes))
    }
    producer.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPartitionPauseAndResume(quorum: String, groupProtocol: String): Unit = {
    val partitions = List(tp).asJava
    val producer = createProducer()
    var startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(partitions)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 0, startingTimestamp = startingTimestamp)
    consumer.pause(partitions)
    startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)
    assertTrue(consumer.poll(Duration.ofMillis(100)).isEmpty)
    consumer.resume(partitions)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 5, startingTimestamp = startingTimestamp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testInterceptors(quorum: String, groupProtocol: String): Unit = {
    val appendStr = "mock"
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()

    // create producer with interceptor
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, classOf[MockProducerInterceptor].getName)
    producerProps.put("mock.interceptor.append", appendStr)
    val testProducer = createProducer(keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer,
      configOverrides = producerProps)

    // produce records
    val numRecords = 10
    (0 until numRecords).map { i =>
      testProducer.send(new ProducerRecord(tp.topic, tp.partition, s"key $i", s"value $i"))
    }.foreach(_.get)
    assertEquals(numRecords, MockProducerInterceptor.ONSEND_COUNT.intValue)
    assertEquals(numRecords, MockProducerInterceptor.ON_SUCCESS_COUNT.intValue)
    // send invalid record
    assertThrows(classOf[Throwable], () => testProducer.send(null), () => "Should not allow sending a null record")
    assertEquals(1, MockProducerInterceptor.ON_ERROR_COUNT.intValue, "Interceptor should be notified about exception")
    assertEquals(0, MockProducerInterceptor.ON_ERROR_WITH_METADATA_COUNT.intValue(), "Interceptor should not receive metadata with an exception when record is null")

    // create consumer with interceptor
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = createConsumer(keyDeserializer = new StringDeserializer, valueDeserializer = new StringDeserializer)
    testConsumer.assign(List(tp).asJava)
    testConsumer.seek(tp, 0)

    // consume and verify that values are modified by interceptors
    val records = consumeRecords(testConsumer, numRecords)
    for (i <- 0 until numRecords) {
      val record = records(i)
      assertEquals(s"key $i", new String(record.key))
      assertEquals(s"value $i$appendStr".toUpperCase(Locale.ROOT), new String(record.value))
    }

    // commit sync and verify onCommit is called
    val commitCountBefore = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue
    testConsumer.commitSync(Map[TopicPartition, OffsetAndMetadata]((tp, new OffsetAndMetadata(2L))).asJava)
    assertEquals(2, testConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(commitCountBefore + 1, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue)

    // commit async and verify onCommit is called
    sendAndAwaitAsyncCommit(testConsumer, Some(Map(tp -> new OffsetAndMetadata(5L))))
    assertEquals(5, testConsumer.committed(Set(tp).asJava).get(tp).offset)
    assertEquals(commitCountBefore + 2, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue)

    testConsumer.close()
    testProducer.close()

    // cleanup
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testInterceptorsWithWrongKeyValue(quorum: String, groupProtocol: String): Unit = {
    val appendStr = "mock"
    // create producer with interceptor that has different key and value types from the producer
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockProducerInterceptor")
    producerProps.put("mock.interceptor.append", appendStr)
    val testProducer = createProducer()

    // producing records should succeed
    testProducer.send(new ProducerRecord(tp.topic(), tp.partition(), s"key".getBytes, s"value will not be modified".getBytes))

    // create consumer with interceptor that has different key and value types from the consumer
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    val testConsumer = createConsumer()

    testConsumer.assign(List(tp).asJava)
    testConsumer.seek(tp, 0)

    // consume and verify that values are not modified by interceptors -- their exceptions are caught and logged, but not propagated
    val records = consumeRecords(testConsumer, 1)
    val record = records.head
    assertEquals(s"value will not be modified", new String(record.value()))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumeMessagesWithCreateTime(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 50
    // Test non-compressed messages
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    // Test compressed messages
    sendCompressedMessages(numRecords, tp2)
    consumer.assign(List(tp2).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, tp = tp2, startingOffset = 0)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumeMessagesWithLogAppendTime(quorum: String, groupProtocol: String): Unit = {
    val topicName = "testConsumeMessagesWithLogAppendTime"
    val topicProps = new Properties()
    topicProps.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
    createTopic(topicName, 2, 2, topicProps)

    val startTime = System.currentTimeMillis()
    val numRecords = 50

    // Test non-compressed messages
    val tp1 = new TopicPartition(topicName, 0)
    val producer = createProducer()
    sendRecords(producer, numRecords, tp1)

    val consumer = createConsumer()
    consumer.assign(List(tp1).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, tp = tp1, startingOffset = 0,
      startingTimestamp = startTime, timestampType = TimestampType.LOG_APPEND_TIME)

    // Test compressed messages
    val tp2 = new TopicPartition(topicName, 1)
    sendCompressedMessages(numRecords, tp2)
    consumer.assign(List(tp2).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, tp = tp2, startingOffset = 0,
      startingTimestamp = startTime, timestampType = TimestampType.LOG_APPEND_TIME)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testListTopics(quorum: String, groupProtocol: String): Unit = {
    val numParts = 2
    val topic1 = "part-test-topic-1"
    val topic2 = "part-test-topic-2"
    val topic3 = "part-test-topic-3"
    createTopic(topic1, numParts)
    createTopic(topic2, numParts)
    createTopic(topic3, numParts)

    val consumer = createConsumer()
    val topics = consumer.listTopics()
    assertNotNull(topics)
    assertEquals(5, topics.size())
    assertEquals(5, topics.keySet().size())
    assertEquals(2, topics.get(topic1).size)
    assertEquals(2, topics.get(topic2).size)
    assertEquals(2, topics.get(topic3).size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPauseStateNotPreservedByRebalance(quorum: String, groupProtocol: String): Unit = {
    if (groupProtocol.equals(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100") // timeout quickly to avoid slow test
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "30")
    }
    val consumer = createConsumer()

    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords = 5, tp, startingTimestamp = startingTimestamp)
    consumer.subscribe(List(topic).asJava)
    consumeAndVerifyRecords(consumer = consumer, numRecords = 5, startingOffset = 0, startingTimestamp = startingTimestamp)
    consumer.pause(List(tp).asJava)

    // subscribe to a new topic to trigger a rebalance
    consumer.subscribe(List("topic2").asJava)

    // after rebalance, our position should be reset and our pause state lost,
    // so we should be able to consume from the beginning
    consumeAndVerifyRecords(consumer = consumer, numRecords = 0, startingOffset = 5, startingTimestamp = startingTimestamp)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPerPartitionLeadMetricsCleanUpWithSubscribe(quorum: String, groupProtocol: String): Unit = {
    val numMessages = 1000
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    // Test subscribe
    // Create a consumer and consumer some messages.
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener
    consumer.subscribe(List(topic, topic2).asJava, listener)
    val records = awaitNonEmptyRecords(consumer, tp)
    assertEquals(1, listener.callsToAssigned, "should be assigned once")
    // Verify the metric exist.
    val tags1 = new util.HashMap[String, String]()
    tags1.put("client-id", "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    tags1.put("topic", tp.topic())
    tags1.put("partition", String.valueOf(tp.partition()))

    val tags2 = new util.HashMap[String, String]()
    tags2.put("client-id", "testPerPartitionLeadMetricsCleanUpWithSubscribe")
    tags2.put("topic", tp2.topic())
    tags2.put("partition", String.valueOf(tp2.partition()))
    val fetchLead0 = consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags1))
    assertNotNull(fetchLead0)
    assertEquals(records.count.toDouble, fetchLead0.metricValue(), s"The lead should be ${records.count}")

    // Remove topic from subscription
    consumer.subscribe(List(topic2).asJava, listener)
    awaitRebalance(consumer, listener)
    // Verify the metric has gone
    assertNull(consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags1)))
    assertNull(consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags2)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPerPartitionLagMetricsCleanUpWithSubscribe(quorum: String, groupProtocol: String): Unit = {
    val numMessages = 1000
    val topic2 = "topic2"
    createTopic(topic2, 2, brokerCount)
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    // Test subscribe
    // Create a consumer and consumer some messages.
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithSubscribe")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithSubscribe")
    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener
    consumer.subscribe(List(topic, topic2).asJava, listener)
    val records = awaitNonEmptyRecords(consumer, tp)
    assertEquals(1, listener.callsToAssigned, "should be assigned once")
    // Verify the metric exist.
    val tags1 = new util.HashMap[String, String]()
    tags1.put("client-id", "testPerPartitionLagMetricsCleanUpWithSubscribe")
    tags1.put("topic", tp.topic())
    tags1.put("partition", String.valueOf(tp.partition()))

    val tags2 = new util.HashMap[String, String]()
    tags2.put("client-id", "testPerPartitionLagMetricsCleanUpWithSubscribe")
    tags2.put("topic", tp2.topic())
    tags2.put("partition", String.valueOf(tp2.partition()))
    val fetchLag0 = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags1))
    assertNotNull(fetchLag0)
    val expectedLag = numMessages - records.count
    assertEquals(expectedLag, fetchLag0.metricValue.asInstanceOf[Double], epsilon, s"The lag should be $expectedLag")

    // Remove topic from subscription
    consumer.subscribe(List(topic2).asJava, listener)
    awaitRebalance(consumer, listener)
    // Verify the metric has gone
    assertNull(consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags1)))
    assertNull(consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags2)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPerPartitionLeadMetricsCleanUpWithAssign(quorum: String, groupProtocol: String): Unit = {
    val numMessages = 1000
    // Test assign
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    sendRecords(producer, numMessages, tp2)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithAssign")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLeadMetricsCleanUpWithAssign")
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val records = awaitNonEmptyRecords(consumer, tp)
    // Verify the metric exist.
    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLeadMetricsCleanUpWithAssign")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val fetchLead = consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags))
    assertNotNull(fetchLead)

    assertEquals(records.count.toDouble, fetchLead.metricValue(), s"The lead should be ${records.count}")

    consumer.assign(List(tp2).asJava)
    awaitNonEmptyRecords(consumer ,tp2)
    assertNull(consumer.metrics.get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPerPartitionLagMetricsCleanUpWithAssign(quorum: String, groupProtocol: String): Unit = {
    val numMessages = 1000
    // Test assign
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    sendRecords(producer, numMessages, tp2)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val records = awaitNonEmptyRecords(consumer, tp)
    // Verify the metric exist.
    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLagMetricsCleanUpWithAssign")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val fetchLag = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags))
    assertNotNull(fetchLag)

    val expectedLag = numMessages - records.count
    assertEquals(expectedLag, fetchLag.metricValue.asInstanceOf[Double], epsilon, s"The lag should be $expectedLag")

    consumer.assign(List(tp2).asJava)
    awaitNonEmptyRecords(consumer, tp2)
    assertNull(consumer.metrics.get(new MetricName(tp.toString + ".records-lag", "consumer-fetch-manager-metrics", "", tags)))
    assertNull(consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testPerPartitionLagMetricsWhenReadCommitted(quorum: String, groupProtocol: String): Unit = {
    val numMessages = 1000
    // send some messages.
    val producer = createProducer()
    sendRecords(producer, numMessages, tp)
    sendRecords(producer, numMessages, tp2)

    consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testPerPartitionLagMetricsCleanUpWithAssign")
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    awaitNonEmptyRecords(consumer, tp)
    // Verify the metric exist.
    val tags = new util.HashMap[String, String]()
    tags.put("client-id", "testPerPartitionLagMetricsCleanUpWithAssign")
    tags.put("topic", tp.topic())
    tags.put("partition", String.valueOf(tp.partition()))
    val fetchLag = consumer.metrics.get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags))
    assertNotNull(fetchLag)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testQuotaMetricsNotCreatedIfNoQuotasConfigured(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 1000
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    sendRecords(producer, numRecords, tp, startingTimestamp = startingTimestamp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0, startingTimestamp = startingTimestamp)

    def assertNoMetric(broker: KafkaBroker, name: String, quotaType: QuotaType, clientId: String): Unit = {
        val metricName = broker.metrics.metricName("throttle-time",
                                  quotaType.toString,
                                  "",
                                  "user", "",
                                  "client-id", clientId)
        assertNull(broker.metrics.metric(metricName), "Metric should not have been created " + metricName)
    }
    brokers.foreach(assertNoMetric(_, "byte-rate", QuotaType.PRODUCE, producerClientId))
    brokers.foreach(assertNoMetric(_, "throttle-time", QuotaType.PRODUCE, producerClientId))
    brokers.foreach(assertNoMetric(_, "byte-rate", QuotaType.FETCH, consumerClientId))
    brokers.foreach(assertNoMetric(_, "throttle-time", QuotaType.FETCH, consumerClientId))

    brokers.foreach(assertNoMetric(_, "request-time", QuotaType.REQUEST, producerClientId))
    brokers.foreach(assertNoMetric(_, "throttle-time", QuotaType.REQUEST, producerClientId))
    brokers.foreach(assertNoMetric(_, "request-time", QuotaType.REQUEST, consumerClientId))
    brokers.foreach(assertNoMetric(_, "throttle-time", QuotaType.REQUEST, consumerClientId))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsumingWithNullGroupId(quorum: String, groupProtocol: String): Unit = {
    val topic = "test_topic"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    createTopic(topic)

    val producer = createProducer()
    producer.send(new ProducerRecord(topic, partition, "k1".getBytes, "v1".getBytes)).get()
    producer.send(new ProducerRecord(topic, partition, "k2".getBytes, "v2".getBytes)).get()
    producer.send(new ProducerRecord(topic, partition, "k3".getBytes, "v3".getBytes)).get()
    producer.close()

    // consumer 1 uses the default group id and consumes from earliest offset
    val consumer1Config = new Properties(consumerConfig)
    consumer1Config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumer1Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer1")
    val consumer1 = createConsumer(
      configOverrides = consumer1Config,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))

    // consumer 2 uses the default group id and consumes from latest offset
    val consumer2Config = new Properties(consumerConfig)
    consumer2Config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumer2Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer2")
    val consumer2 = createConsumer(
      configOverrides = consumer2Config,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))

    // consumer 3 uses the default group id and starts from an explicit offset
    val consumer3Config = new Properties(consumerConfig)
    consumer3Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer3")
    val consumer3 = createConsumer(
      configOverrides = consumer3Config,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))

    consumer1.assign(asList(tp))
    consumer2.assign(asList(tp))
    consumer3.assign(asList(tp))
    consumer3.seek(tp, 1)

    val numRecords1 = consumer1.poll(Duration.ofMillis(5000)).count()
    assertThrows(classOf[InvalidGroupIdException], () => consumer1.commitSync())
    assertThrows(classOf[InvalidGroupIdException], () => consumer2.committed(Set(tp).asJava))

    val numRecords2 = consumer2.poll(Duration.ofMillis(5000)).count()
    val numRecords3 = consumer3.poll(Duration.ofMillis(5000)).count()

    consumer1.unsubscribe()
    consumer2.unsubscribe()
    consumer3.unsubscribe()

    assertTrue(consumer1.assignment().isEmpty)
    assertTrue(consumer2.assignment().isEmpty)
    assertTrue(consumer3.assignment().isEmpty)

    consumer1.close()
    consumer2.close()
    consumer3.close()

    assertEquals(3, numRecords1, "Expected consumer1 to consume from earliest offset")
    assertEquals(0, numRecords2, "Expected consumer2 to consume from latest offset")
    assertEquals(2, numRecords3, "Expected consumer3 to consume from offset 1")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testNullGroupIdNotSupportedIfCommitting(quorum: String, groupProtocol: String): Unit = {
    val consumer1Config = new Properties(consumerConfig)
    consumer1Config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumer1Config.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer1")
    val consumer1 = createConsumer(
      configOverrides = consumer1Config,
      configsToRemove = List(ConsumerConfig.GROUP_ID_CONFIG))

    consumer1.assign(List(tp).asJava)
    assertThrows(classOf[InvalidGroupIdException], () => consumer1.commitSync())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testStaticConsumerDetectsNewPartitionCreatedAfterRestart(quorum:String, groupProtocol: String): Unit = {
    val foo = "foo"
    val foo0 = new TopicPartition(foo, 0)
    val foo1 = new TopicPartition(foo, 1)

    val admin = createAdminClient()
    admin.createTopics(Seq(new NewTopic(foo, 1, 1.toShort)).asJava).all.get

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id")
    consumerConfig.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id")

    val consumer1 = createConsumer(configOverrides = consumerConfig)
    consumer1.subscribe(Seq(foo).asJava)
    awaitAssignment(consumer1, Set(foo0))
    consumer1.close()

    val consumer2 = createConsumer(configOverrides = consumerConfig)
    consumer2.subscribe(Seq(foo).asJava)
    awaitAssignment(consumer2, Set(foo0))

    admin.createPartitions(Map(foo -> NewPartitions.increaseTo(2)).asJava).all.get

    awaitAssignment(consumer2, Set(foo0, foo1))

    consumer2.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testEndOffsets(quorum: String, groupProtocol: String): Unit = {
    val producer = createProducer()
    val startingTimestamp = System.currentTimeMillis()
    val numRecords = 10000
    (0 until numRecords).map { i =>
      val timestamp = startingTimestamp + i.toLong
      val record = new ProducerRecord(tp.topic(), tp.partition(), timestamp, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
      record
    }
    producer.flush()

    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    awaitAssignment(consumer, Set(tp, tp2))

    val endOffsets = consumer.endOffsets(Set(tp).asJava)
    assertEquals(numRecords, endOffsets.get(tp))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testSeekThrowsIllegalStateIfPartitionsNotAssigned(quorum: String, groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic, 0)
    val consumer = createConsumer(configOverrides = consumerConfig)
    val e: Exception = assertThrows(classOf[IllegalStateException], () => consumer.seekToEnd(Collections.singletonList(tp)))
    assertEquals("No current assignment for partition " + tp, e.getMessage)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchOffsetsForTime(quorum: String, groupProtocol: String): Unit = {
    val numPartitions = 2
    val producer = createProducer()
    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    var i = 0
    for (part <- 0 until numPartitions) {
      val tp = new TopicPartition(topic, part)
      // key, val, and timestamp equal to the sequence number.
      sendRecords(producer, numRecords = 100, tp, startingTimestamp = 0)
      timestampsToSearch.put(tp, (i * 20).toLong)
      i += 1
    }

    val consumer = createConsumer()
    // Test negative target time
    assertThrows(classOf[IllegalArgumentException],
      () => consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(topic, 0), -1)))
    val timestampOffsets = consumer.offsetsForTimes(timestampsToSearch)

    val timestampTp0 = timestampOffsets.get(new TopicPartition(topic, 0))
    assertEquals(0, timestampTp0.offset)
    assertEquals(0, timestampTp0.timestamp)
    assertEquals(Optional.of(0), timestampTp0.leaderEpoch)

    val timestampTp1 = timestampOffsets.get(new TopicPartition(topic, 1))
    assertEquals(20, timestampTp1.offset)
    assertEquals(20, timestampTp1.timestamp)
    assertEquals(Optional.of(0), timestampTp1.leaderEpoch)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  @Timeout(15)
  def testPositionRespectsTimeout(quorum: String, groupProtocol: String): Unit = {
    val topicPartition = new TopicPartition(topic, 15)
    val consumer = createConsumer()
    consumer.assign(List(topicPartition).asJava)

    // When position() is called for a topic/partition that doesn't exist, the consumer will repeatedly update the
    // local metadata. However, it should give up after the user-supplied timeout has past.
    assertThrows(classOf[TimeoutException], () => consumer.position(topicPartition, Duration.ofSeconds(3)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  @Timeout(15)
  def testPositionRespectsWakeup(quorum: String, groupProtocol: String): Unit = {
    val topicPartition = new TopicPartition(topic, 15)
    val consumer = createConsumer()
    consumer.assign(List(topicPartition).asJava)

    CompletableFuture.runAsync { () =>
      TimeUnit.SECONDS.sleep(1)
      consumer.wakeup()
    }

    assertThrows(classOf[WakeupException], () => consumer.position(topicPartition, Duration.ofSeconds(3)))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  @Timeout(15)
  def testPositionWithErrorConnectionRespectsWakeup(quorum: String, groupProtocol: String): Unit = {
    val topicPartition = new TopicPartition(topic, 15)
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:12345") // make sure the connection fails
    val consumer = createConsumer(configOverrides = properties)
    consumer.assign(List(topicPartition).asJava)

    CompletableFuture.runAsync { () =>
      TimeUnit.SECONDS.sleep(1)
      consumer.wakeup()
    }

    assertThrows(classOf[WakeupException], () => consumer.position(topicPartition, Duration.ofSeconds(100)))
  }

  @Flaky("KAFKA-18031")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testCloseLeavesGroupOnInterrupt(quorum: String, groupProtocol: String): Unit = {
    val adminClient = createAdminClient()
    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener()
    consumer.subscribe(List(topic).asJava, listener)
    awaitRebalance(consumer, listener)

    assertEquals(1, listener.callsToAssigned)
    assertEquals(0, listener.callsToRevoked)

    try {
      Thread.currentThread().interrupt()
      assertThrows(classOf[InterruptException], () => consumer.close())
    } finally {
      // Clear the interrupted flag so we don't create problems for subsequent tests.
      Thread.interrupted()
    }

    assertEquals(1, listener.callsToAssigned)
    assertEquals(1, listener.callsToRevoked)

    val config = new ConsumerConfig(consumerConfig)

    // Set the wait timeout to be only *half* the configured session timeout. This way we can make sure that the
    // consumer explicitly left the group as opposed to being kicked out by the broker.
    val leaveGroupTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) / 2

    TestUtils.waitUntilTrue(
      () => {
        try {
          val groupId = config.getString(ConsumerConfig.GROUP_ID_CONFIG)
          val groupDescription = adminClient.describeConsumerGroups (Collections.singletonList (groupId) ).describedGroups.get (groupId).get
          groupDescription.members.isEmpty
        } catch {
          case _: ExecutionException | _: InterruptedException =>
            false
        }
      },
      msg=s"Consumer did not leave the consumer group within $leaveGroupTimeoutMs ms of close",
      waitTimeMs=leaveGroupTimeoutMs
    )
  }
}
