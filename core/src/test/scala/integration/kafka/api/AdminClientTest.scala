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

import kafka.admin.AdminClient
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, Logging}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.junit.{Before, Test}
import org.junit.Assert._
import scala.collection.JavaConversions._

class AdminClientTest extends IntegrationTestHarness with Logging {

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
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")

  @Before
  override def setUp() {
    super.setUp
    client = AdminClient.createSimplePlaintext(this.brokerList)
    TestUtils.createTopic(this.zkUtils, topic, 2, serverCount, this.servers)
  }

  @Test
  def testListGroups() {
    consumers.head.subscribe(List(topic))
    TestUtils.waitUntilTrue(() => {
      consumers.head.poll(0)
      !consumers.head.assignment().isEmpty
    }, "Expected non-empty assignment")

    val groups = client.listAllGroupsFlattened
    assertFalse(groups.isEmpty)
    val group = groups.head
    assertEquals(groupId, group.groupId)
    assertEquals("consumer", group.protocolType)
  }

  @Test
  def testDescribeGroup() {
    consumers.head.subscribe(List(topic))
    TestUtils.waitUntilTrue(() => {
      consumers.head.poll(0)
      !consumers.head.assignment().isEmpty
    }, "Expected non-empty assignment")

    val group = client.describeGroup(groupId)
    assertEquals("consumer", group.protocolType)
    assertEquals("range", group.protocol)
    assertEquals("Stable", group.state)
    assertFalse(group.members.isEmpty)

    val member = group.members.head
    assertEquals(clientId, member.clientId)
    assertFalse(member.clientHost.isEmpty)
    assertFalse(member.memberId.isEmpty)
  }

  @Test
  def testDescribeConsumerGroup() {
    consumers.head.subscribe(List(topic))
    TestUtils.waitUntilTrue(() => {
      consumers.head.poll(0)
      !consumers.head.assignment().isEmpty
    }, "Expected non-empty assignment")

    val consumerSummaries = client.describeConsumerGroup(groupId)
    assertEquals(1, consumerSummaries.size)
    assertEquals(Some(Set(tp, tp2)), consumerSummaries.map(_.head.assignment.toSet))
  }

  @Test
  def testDescribeConsumerGroupForNonExistentGroup() {
    val nonExistentGroup = "non" + groupId
    assertTrue("Expected empty ConsumerSummary list", client.describeConsumerGroup(nonExistentGroup).isEmpty)
  }
}
