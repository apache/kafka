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

import java.util.{Optional, Properties}
import kafka.log.LogConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.jdk.CollectionConverters._

/**
 * This test verifies that the KIP-541 broker-level FetchMaxBytes configuration is honored.
 */
class FetchRequestMaxBytesTest extends BaseRequestTest {
  override def brokerCount: Int = 1

  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  private val testTopic = "testTopic"
  private val testTopicPartition = new TopicPartition(testTopic, 0)
  private val messages = IndexedSeq(
    multiByteArray(1),
    multiByteArray(500),
    multiByteArray(1040),
    multiByteArray(500),
    multiByteArray(50))

  private def multiByteArray(length: Int): Array[Byte] = {
    val array = new Array[Byte](length)
    array.indices.foreach(i => array(i) = (i % 5).toByte)
    array
  }

  private def oneByteArray(value: Byte): Array[Byte] = {
    val array = new Array[Byte](1)
    array(0) = value
    array
  }

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers))
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    super.tearDown()
  }

  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)
    properties.put(KafkaConfig.FetchMaxBytes, "1024")
  }

  private def createTopics(): Unit = {
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, 1.toString)
    createTopic(testTopic,
                numPartitions = 1, 
                replicationFactor = 1,
                topicConfig = topicConfig)
    // Produce several messages as single batches.
    messages.indices.foreach(i => {
      val record = new ProducerRecord(testTopic, 0, oneByteArray(i.toByte), messages(i))
      val future = producer.send(record)
      producer.flush()
      future.get()
    })
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse[MemoryRecords] = {
    connectAndReceive[FetchResponse[MemoryRecords]](request, destination = brokerSocketServer(leaderId))
  }

  /**
   * Tests that each of our fetch requests respects FetchMaxBytes.
   *
   * Note that when a single batch is larger than FetchMaxBytes, it will be
   * returned in full even if this is larger than FetchMaxBytes.  See KIP-74.
   */
  @Test
  def testConsumeMultipleRecords(): Unit = {
    createTopics()

    expectNextRecords(IndexedSeq(messages(0), messages(1)), 0)
    expectNextRecords(IndexedSeq(messages(2)), 2)
    expectNextRecords(IndexedSeq(messages(3), messages(4)), 3)
  }

  private def expectNextRecords(expected: IndexedSeq[Array[Byte]],
                                fetchOffset: Long): Unit = {
    val response = sendFetchRequest(0,
      FetchRequest.Builder.forConsumer(Int.MaxValue, 0,
        Map(testTopicPartition ->
          new PartitionData(fetchOffset, 0, Integer.MAX_VALUE, Optional.empty())).asJava).build(3))
    val records = response.responseData().get(testTopicPartition).records.records()
    assertNotNull(records)
    val recordsList = records.asScala.toList
    assertEquals(expected.size, recordsList.size)
    recordsList.zipWithIndex.foreach {
      case (record, i) => {
        val buffer = record.value().duplicate()
        val array = new Array[Byte](buffer.remaining())
        buffer.get(array)
        assertArrayEquals(expected(i),
          array, s"expectNextRecords unexpected element ${i}")
      }
    }
  }
}
