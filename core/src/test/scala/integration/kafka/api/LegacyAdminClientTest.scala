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
package kafka.api

import java.util.Collections
import java.util.concurrent.TimeUnit

import kafka.admin.AdminClient
import kafka.admin.AdminClient.DeleteRecordsResult
import kafka.server.KafkaConfig
import java.lang.{Long => JLong}
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{Errors, ApiKeys}
import org.apache.kafka.common.requests.DeleteRecordsRequest
import org.junit.{After, Before, Test}
import org.junit.Assert._
import scala.collection.JavaConverters._

/**
  * Tests for the deprecated Scala AdminClient.
  */
class LegacyAdminClientTest extends IntegrationTestHarness with Logging {

  val producerCount = 1
  val consumerCount = 2
  val serverCount = 3
  val groupId = "my-test"
  val clientId = "consumer-498"

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val part2 = 1
  val tp2 = new TopicPartition(topic, part2)

  var client: AdminClient = null

  // configure the servers and clients
  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100") // set small enough session timeout
  this.serverConfig.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0") // do initial rebalance immediately
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")

  @Before
  override def setUp() {
    super.setUp()
    client = AdminClient.createSimplePlaintext(this.brokerList)
    TestUtils.createTopic(this.zkUtils, topic, 2, serverCount, this.servers)
  }

  @After
  override def tearDown() {
    client.close()
    super.tearDown()
  }

  @Test
  def testSeekToBeginningAfterDeleteRecords() {
    val consumer = consumers.head
    subscribeAndWaitForAssignment(topic, consumer)

    sendRecords(producers.head, 10, tp)
    consumer.seekToBeginning(Collections.singletonList(tp))
    assertEquals(0L, consumer.position(tp))

    client.deleteRecordsBefore(Map((tp, 5L))).get()
    consumer.seekToBeginning(Collections.singletonList(tp))
    assertEquals(5L, consumer.position(tp))

    client.deleteRecordsBefore(Map((tp, DeleteRecordsRequest.HIGH_WATERMARK))).get()
    consumer.seekToBeginning(Collections.singletonList(tp))
    assertEquals(10L, consumer.position(tp))
  }

  @Test
  def testConsumeAfterDeleteRecords() {
    val consumer = consumers.head
    subscribeAndWaitForAssignment(topic, consumer)

    sendRecords(producers.head, 10, tp)
    var messageCount = 0
    TestUtils.waitUntilTrue(() => {
      messageCount += consumer.poll(0).count()
      messageCount == 10
    }, "Expected 10 messages", 3000L)

    client.deleteRecordsBefore(Map((tp, 3L))).get()
    consumer.seek(tp, 1)
    messageCount = 0
    TestUtils.waitUntilTrue(() => {
      messageCount += consumer.poll(0).count()
      messageCount == 7
    }, "Expected 7 messages", 3000L)

    client.deleteRecordsBefore(Map((tp, 8L))).get()
    consumer.seek(tp, 1)
    messageCount = 0
    TestUtils.waitUntilTrue(() => {
      messageCount += consumer.poll(0).count()
      messageCount == 2
    }, "Expected 2 messages", 3000L)
  }

  @Test
  def testLogStartOffsetCheckpoint() {
    subscribeAndWaitForAssignment(topic, consumers.head)

    sendRecords(producers.head, 10, tp)
    assertEquals(DeleteRecordsResult(5L, null), client.deleteRecordsBefore(Map((tp, 5L))).get()(tp))

    for (i <- 0 until serverCount)
      killBroker(i)
    restartDeadBrokers()

    client.close()
    brokerList = TestUtils.bootstrapServers(servers, listenerName)
    client = AdminClient.createSimplePlaintext(brokerList)

    TestUtils.waitUntilTrue(() => {
      // Need to retry if leader is not available for the partition
      client.deleteRecordsBefore(Map((tp, 0L))).get(1000L, TimeUnit.MILLISECONDS)(tp).equals(DeleteRecordsResult(5L, null))
    }, "Expected low watermark of the partition to be 5L")
  }

  @Test
  def testLogStartOffsetAfterDeleteRecords() {
    subscribeAndWaitForAssignment(topic, consumers.head)

    sendRecords(producers.head, 10, tp)
    client.deleteRecordsBefore(Map((tp, 3L))).get()

    for (i <- 0 until serverCount)
      assertEquals(3, servers(i).replicaManager.getReplica(tp).get.logStartOffset)
  }

  @Test
  def testOffsetsForTimesWhenOffsetNotFound() {
    val consumer = consumers.head
    assertNull(consumer.offsetsForTimes(Map(tp -> new JLong(0L)).asJava).get(tp))
  }

  @Test
  def testOffsetsForTimesAfterDeleteRecords() {
    val consumer = consumers.head
    subscribeAndWaitForAssignment(topic, consumer)

    sendRecords(producers.head, 10, tp)
    assertEquals(0L, consumer.offsetsForTimes(Map(tp -> new JLong(0L)).asJava).get(tp).offset())

    client.deleteRecordsBefore(Map((tp, 5L))).get()
    assertEquals(5L, consumer.offsetsForTimes(Map(tp -> new JLong(0L)).asJava).get(tp).offset())

    client.deleteRecordsBefore(Map((tp, DeleteRecordsRequest.HIGH_WATERMARK))).get()
    assertNull(consumer.offsetsForTimes(Map(tp -> new JLong(0L)).asJava).get(tp))
  }

  @Test
  def testDeleteRecordsWithException() {
    subscribeAndWaitForAssignment(topic, consumers.head)

    sendRecords(producers.head, 10, tp)
    // Should get success result
    assertEquals(DeleteRecordsResult(5L, null), client.deleteRecordsBefore(Map((tp, 5L))).get()(tp))
    // OffsetOutOfRangeException if offset > high_watermark
    assertEquals(DeleteRecordsResult(-1L, Errors.OFFSET_OUT_OF_RANGE.exception()), client.deleteRecordsBefore(Map((tp, 20))).get()(tp))

    val nonExistPartition = new TopicPartition(topic, 3)
    // UnknownTopicOrPartitionException if user tries to delete records of a non-existent partition
    assertEquals(DeleteRecordsResult(-1L, Errors.LEADER_NOT_AVAILABLE.exception()),
                 client.deleteRecordsBefore(Map((nonExistPartition, 20))).get()(nonExistPartition))
  }

  @Test
  def testListGroups() {
    subscribeAndWaitForAssignment(topic, consumers.head)

    val groups = client.listAllGroupsFlattened
    assertFalse(groups.isEmpty)
    val group = groups.head
    assertEquals(groupId, group.groupId)
    assertEquals("consumer", group.protocolType)
  }

  @Test
  def testListAllBrokerVersionInfo() {
    subscribeAndWaitForAssignment(topic, consumers.head)

    val brokerVersionInfos = client.listAllBrokerVersionInfo
    val brokers = brokerList.split(",")
    assertEquals(brokers.size, brokerVersionInfos.size)
    for ((node, tryBrokerVersionInfo) <- brokerVersionInfos) {
      val hostStr = s"${node.host}:${node.port}"
      assertTrue(s"Unknown host:port pair $hostStr in brokerVersionInfos", brokers.contains(hostStr))
      val brokerVersionInfo = tryBrokerVersionInfo.get
      assertEquals(1, brokerVersionInfo.latestUsableVersion(ApiKeys.API_VERSIONS))
    }
  }

  @Test
  def testGetConsumerGroupSummary() {
    subscribeAndWaitForAssignment(topic, consumers.head)

    val group = client.describeConsumerGroup(groupId)
    assertEquals("range", group.assignmentStrategy)
    assertEquals("Stable", group.state)
    assertFalse(group.consumers.isEmpty)

    val member = group.consumers.get.head
    assertEquals(clientId, member.clientId)
    assertFalse(member.host.isEmpty)
    assertFalse(member.consumerId.isEmpty)
  }

  @Test
  def testDescribeConsumerGroup() {
    subscribeAndWaitForAssignment(topic, consumers.head)

    val consumerGroupSummary = client.describeConsumerGroup(groupId)
    assertEquals(1, consumerGroupSummary.consumers.get.size)
    assertEquals(List(tp, tp2), consumerGroupSummary.consumers.get.flatMap(_.assignment))
  }

  @Test
  def testDescribeConsumerGroupForNonExistentGroup() {
    val nonExistentGroup = "non" + groupId
    assertTrue("Expected empty ConsumerSummary list", client.describeConsumerGroup(nonExistentGroup).consumers.get.isEmpty)
  }

  private def subscribeAndWaitForAssignment(topic: String, consumer: KafkaConsumer[Array[Byte], Array[Byte]]) {
    consumer.subscribe(Collections.singletonList(topic))
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0)
      !consumer.assignment.isEmpty
    }, "Expected non-empty assignment")
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                          numRecords: Int,
                          tp: TopicPartition) {
    val futures = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      producer.send(record)
    }

    futures.foreach(_.get)
  }

}
