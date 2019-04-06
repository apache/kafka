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

import kafka.admin.AdminClient
import kafka.server.KafkaConfig
import java.lang.{Long => JLong}

import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.{After, Before, Test}
import org.junit.Assert._

import scala.collection.JavaConverters._

/**
  * Tests for the deprecated Scala AdminClient.
  */
@deprecated("The Scala AdminClient has been deprecated in favour of org.apache.kafka.clients.admin.AdminClient",
  since = "0.11.0")
class LegacyAdminClientTest extends IntegrationTestHarness with Logging {

  val producerCount = 1
  val consumerCount = 2
  val brokerCount = 3
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
    createTopic(topic, 2, brokerCount)
  }

  @After
  override def tearDown() {
    client.close()
    super.tearDown()
  }

  @Test
  def testOffsetsForTimesWhenOffsetNotFound() {
    val consumer = createConsumer()
    assertNull(consumer.offsetsForTimes(Map(tp -> JLong.valueOf(0L)).asJava).get(tp))
  }

  @Test
  def testListGroups() {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val groups = client.listAllGroupsFlattened
    assertFalse(groups.isEmpty)
    val group = groups.head
    assertEquals(groupId, group.groupId)
    assertEquals("consumer", group.protocolType)
  }

  @Test
  def testListAllBrokerVersionInfo() {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val brokerVersionInfos = client.listAllBrokerVersionInfo
    val brokers = brokerList.split(",")
    assertEquals(brokers.size, brokerVersionInfos.size)
    for ((node, tryBrokerVersionInfo) <- brokerVersionInfos) {
      val hostStr = s"${node.host}:${node.port}"
      assertTrue(s"Unknown host:port pair $hostStr in brokerVersionInfos", brokers.contains(hostStr))
      val brokerVersionInfo = tryBrokerVersionInfo.get
      assertEquals(2, brokerVersionInfo.latestUsableVersion(ApiKeys.API_VERSIONS))
    }
  }

  @Test
  def testGetConsumerGroupSummary() {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

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
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

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

}
