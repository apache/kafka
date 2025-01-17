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

package kafka.coordinator.transaction

import kafka.network.SocketServer
import kafka.server.IntegrationTestUtils
import org.apache.kafka.clients.admin.{Admin, NewTopic, TransactionState}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterFeature, ClusterTest, ClusterTestDefaults, ClusterTests, Type}
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{InitProducerIdRequest, InitProducerIdResponse}
import org.apache.kafka.common.test.{ClusterInstance, TestUtils}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{Feature, MetadataVersion}
import org.junit.jupiter.api.Assertions.{assertEquals, assertInstanceOf, assertThrows, assertTrue}

import java.time.Duration
import java.util
import java.util.Collections
import java.util.concurrent.ExecutionException
import java.util.stream.{Collectors, IntStream, StreamSupport}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

@ClusterTestDefaults(types = Array(Type.KRAFT), serverProperties = Array(
  new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
  new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
  new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
  new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
  new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
))
class ProducerIntegrationTest {

  @ClusterTests(Array(
    new ClusterTest(metadataVersion = MetadataVersion.IBP_3_3_IV0)
  ))
  def testUniqueProducerIds(clusterInstance: ClusterInstance): Unit = {
    verifyUniqueIds(clusterInstance)
  }

  @ClusterTests(Array(
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0))),
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1))),
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2))),
  ))
  def testTransactionWithAndWithoutSend(cluster: ClusterInstance): Unit = {
    val properties = new util.HashMap[String, Object]
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "foobar")
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    val producer: Producer[Array[Byte], Array[Byte]] = cluster.producer(properties)
    try {
      producer.initTransactions()
      producer.beginTransaction()
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("test", "key".getBytes, "value".getBytes))
      producer.commitTransaction()

      producer.beginTransaction()
      producer.commitTransaction()
    } finally if (producer != null) producer.close()
  }

  @ClusterTests(Array(
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0))),
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1))),
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2))),
  ))
  def testTransactionWithInvalidSendAndEndTxnRequestSent(cluster: ClusterInstance): Unit = {
    val topic = new NewTopic("foobar", 1, 1.toShort).configs(Collections.singletonMap(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100"))
    val txnId = "test-txn"
    val properties = new util.HashMap[String, Object]
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId)
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val admin = cluster.admin()
    val producer: Producer[Array[Byte], Array[Byte]] = cluster.producer(properties)
    try {
      admin.createTopics(List(topic).asJava)

      producer.initTransactions()
      producer.beginTransaction()
      assertInstanceOf(classOf[RecordTooLargeException],
        assertThrows(classOf[ExecutionException],
          () => producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
            topic.name(), Array.fill(100)(0: Byte), Array.fill(100)(0: Byte))).get()).getCause)

      producer.abortTransaction()
    } finally {
      if (admin != null) admin.close()
      if (producer != null) producer.close()
    }
  }

  @ClusterTests(Array(
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0))),
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1))),
    new ClusterTest(features = Array(
      new ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2))),
  ))
  def testTransactionWithSendOffset(cluster: ClusterInstance): Unit = {
    val inputTopic: String = "my-input-topic"
    var producer: Producer[Array[Byte], Array[Byte]] = cluster.producer
    try {
      for (i <- 0 until 5) {
        val key: Array[Byte] = ("key-" + i).getBytes
        val value: Array[Byte] = ("value-" + i).getBytes
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](inputTopic, key, value)).get
      }
    } finally if (producer != null) producer.close()

    val txnId: String = "foobar"
    val producerProperties: util.Map[String, Object] = new util.HashMap[String, Object]
    producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId)
    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "test")
    producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val consumerProperties: util.Map[String, Object] = new util.HashMap[String, Object]
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    producer = cluster.producer(producerProperties)
    val consumer: Consumer[Array[Byte], Array[Byte]] = cluster.consumer(consumerProperties)
    try {
      producer.initTransactions()
      producer.beginTransaction()
      consumer.subscribe(util.List.of(inputTopic))
      var records: ConsumerRecords[Array[Byte], Array[Byte]] = null
      TestUtils.waitForCondition(() => {
        records = consumer.poll(Duration.ZERO)
        records.count == 5
      }, "poll records size not match")
      val lastRecord = StreamSupport.stream(records.spliterator, false).reduce((_, second) => second).orElse(null)
      val offsets = Collections.singletonMap(
        new TopicPartition(lastRecord.topic, lastRecord.partition), new OffsetAndMetadata(lastRecord.offset + 1))
      producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata)
      producer.commitTransaction()
    } finally {
      if (producer != null) producer.close()
      if (consumer != null) consumer.close()
    }

    val admin: Admin = cluster.admin
    try {
      TestUtils.waitForCondition(() => {
        admin.listTransactions.all.get.stream
          .filter(txn => txn.transactionalId == txnId)
          .anyMatch(txn => txn.state eq TransactionState.COMPLETE_COMMIT)
      }, "transaction is not in COMPLETE_COMMIT state")
    } finally if (admin != null) admin.close()
  }

  private def verifyUniqueIds(clusterInstance: ClusterInstance): Unit = {
    // Request enough PIDs from each broker to ensure each broker generates two blocks
    val ids = clusterInstance.brokerSocketServers().stream().flatMap( broker => {
      IntStream.range(0, 1001).parallel().mapToObj( _ =>
        nextProducerId(broker, clusterInstance.clientListener())
      )}).collect(Collectors.toList[Long]).asScala.toSeq

    val brokerCount = clusterInstance.brokerIds.size
    val expectedTotalCount = 1001 * brokerCount
    assertEquals(expectedTotalCount, ids.size, s"Expected exactly $expectedTotalCount IDs")
    assertEquals(expectedTotalCount, ids.distinct.size, "Found duplicate producer IDs")
  }

  private def nextProducerId(broker: SocketServer, listener: ListenerName): Long = {
    // Generating producer ids may fail while waiting for the initial block and also
    // when the current block is full and waiting for the prefetched block.
    val deadline = 5.seconds.fromNow
    var shouldRetry = true
    var response: InitProducerIdResponse = null
    while (shouldRetry && deadline.hasTimeLeft()) {
      val data = new InitProducerIdRequestData()
        .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
        .setProducerId(RecordBatch.NO_PRODUCER_ID)
        .setTransactionalId(null)
        .setTransactionTimeoutMs(10)
      val request = new InitProducerIdRequest.Builder(data).build()

      response = IntegrationTestUtils.connectAndReceive[InitProducerIdResponse](request,
        destination = broker,
        listenerName = listener)

      shouldRetry = response.data.errorCode == Errors.COORDINATOR_LOAD_IN_PROGRESS.code
    }
    assertTrue(deadline.hasTimeLeft())
    assertEquals(Errors.NONE.code, response.data.errorCode)
    response.data().producerId()
  }
}
