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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.{Collections, Properties}

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
    "zk,gzip"
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
        TestUtils.createTopicWithAdmin(admin, topic, Seq(broker))
      } finally {
        admin.close()
      }
      val partition = 0

      // prepare the messages
      val messageValues = (0 until numRecords).map(i => "value" + i)

      // make sure the returned messages are correct
      val now = System.currentTimeMillis()
      val responses = for (message <- messageValues)
        yield producer.send(new ProducerRecord(topic, null, now, null, message.getBytes))
      for ((future, offset) <- responses.zipWithIndex) {
        assertEquals(offset.toLong, future.get.offset)
      }

      val tp = new TopicPartition(topic, partition)
      // make sure the fetched message count match
      consumer.assign(Collections.singleton(tp))
      consumer.seek(tp, 0)
      val records = TestUtils.consumeRecords(consumer, numRecords)

      for (((messageValue, record), index) <- messageValues.zip(records).zipWithIndex) {
        assertEquals(messageValue, new String(record.value))
        assertEquals(now, record.timestamp)
        assertEquals(index.toLong, record.offset)
      }
    } finally {
      producer.close()
      consumer.close()
    }
  }
}
