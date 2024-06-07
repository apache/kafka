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

package kafka.api.test

import kafka.server.{KafkaBroker, KafkaConfig, QuorumTestHarness}
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.concurrent.Future
import java.util.{Collections, Properties}
import scala.collection.mutable.ListBuffer
import scala.util.Random

class ProducerCompressionTest extends QuorumTestHarness {

  private val brokerId = 0
  private val topic = "topic"
  private val numRecords = 2000

  private var broker: KafkaBroker = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    val props = TestUtils.createBrokerConfig(brokerId, zkConnectOrNull)
    broker = createBroker(new KafkaConfig(props))
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(Seq(broker))
    super.tearDown()
  }

  /**
   * testCompression
   *
   * Compressed messages should be able to sent and consumed correctly
   */
  @ParameterizedTest
  @CsvSource(value = Array(
    "kraft,none",
    "kraft,gzip",
    "kraft,snappy",
    "kraft,lz4",
    "kraft,zstd",
    "zk,gzip",
    "zk,snappy"
  ))
  def testCompression(quorum: String, compression: String): Unit = {
    val producerProps = new Properties()
    val bootstrapServers = TestUtils.plaintextBootstrapServers(Seq(broker))
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "66000")
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "200")
    val producer = new KafkaProducer(producerProps, new ByteArraySerializer, new ByteArraySerializer)
    val consumer = TestUtils.createConsumer(bootstrapServers)

    try {
      // create topic
      val admin = TestUtils.createAdminClient(Seq(broker),
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
      try {
        TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers)
      } finally {
        admin.close()
      }
      val partition = 0

      def messageValue(length: Int): String = {
        val random = new Random(0)
        new String(random.alphanumeric.take(length).toArray)
      }

      // prepare the messages
      val messageValues = (0 until numRecords).map(i => messageValue(i))
      val headerArr = Array[Header](new RecordHeader("key", "value".getBytes))
      val headers = new RecordHeaders(headerArr)

      // make sure the returned messages are correct
      val now = System.currentTimeMillis()
      val responses: ListBuffer[Future[RecordMetadata]] = new ListBuffer[Future[RecordMetadata]]()

      for (message <- messageValues) {
        // 1. send message without key and header
        responses += producer.send(new ProducerRecord(topic, null, now, null, message.getBytes))
        // 2. send message with key, without header
        responses += producer.send(new ProducerRecord(topic, null, now, message.length.toString.getBytes, message.getBytes))
        // 3. send message with key and header
        responses += producer.send(new ProducerRecord(topic, null, now, message.length.toString.getBytes, message.getBytes, headers))
      }
      for ((future, offset) <- responses.zipWithIndex) {
        assertEquals(offset.toLong, future.get.offset)
      }

      val tp = new TopicPartition(topic, partition)
      // make sure the fetched message count match
      consumer.assign(Collections.singleton(tp))
      consumer.seek(tp, 0)
      val records = TestUtils.consumeRecords(consumer, numRecords*3)

      for (i <- 0 until numRecords) {
        val messageValue = messageValues(i)
        // 1. verify message without key and header
        var offset = i * 3
        var record = records(offset)
        assertNull(record.key())
        assertEquals(messageValue, new String(record.value))
        assertEquals(0, record.headers().toArray.length)
        assertEquals(now, record.timestamp)
        assertEquals(offset.toLong, record.offset)

        // 2. verify message with key, without header
        offset = i * 3 + 1
        record = records(offset)
        assertEquals(messageValue.length.toString, new String(record.key()))
        assertEquals(messageValue, new String(record.value))
        assertEquals(0, record.headers().toArray.length)
        assertEquals(now, record.timestamp)
        assertEquals(offset.toLong, record.offset)

        // 3. verify message with key and header
        offset = i * 3 + 2
        record = records(offset)
        assertEquals(messageValue.length.toString, new String(record.key()))
        assertEquals(messageValue, new String(record.value))
        assertEquals(1, record.headers().toArray.length)
        assertEquals(headerArr.apply(0), record.headers().toArray.apply(0))
        assertEquals(now, record.timestamp)
        assertEquals(offset.toLong, record.offset)
      }
    } finally {
      producer.close()
      consumer.close()
    }
  }
}
