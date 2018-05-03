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
package kafka.admin

import java.nio.charset.StandardCharsets

import kafka.utils._
import kafka.server.KafkaConfig
import org.junit.{After, Before, Test}
import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import kafka.integration.KafkaServerTestHarness
import org.apache.kafka.common.security.JaasUtils


@deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
class DeleteConsumerGroupTest extends KafkaServerTestHarness {
  def generateConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false, true).map(KafkaConfig.fromProps)
  var zkUtils: ZkUtils = null

  @Before
  override def setUp(): Unit = {
    super.setUp()
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled))
  }

  @After
  override def tearDown(): Unit = {
    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close(), this)
    super.tearDown()
  }


  @Test
  def testGroupWideDeleteInZK(): Unit = {
    val topic = "test"
    val groupToDelete = "groupToDelete"
    val otherGroup = "otherGroup"

    createTopic(topic, 1, 3)
    fillInConsumerGroupInfo(topic, groupToDelete, "consumer", 0, 10, false)
    fillInConsumerGroupInfo(topic, otherGroup, "consumer", 0, 10, false)

    AdminUtils.deleteConsumerGroupInZK(zkUtils, groupToDelete)

    TestUtils.waitUntilTrue(() => !groupDirExists(new ZKGroupDirs(groupToDelete)),
      "DeleteConsumerGroupInZK should delete the provided consumer group's directory")
    TestUtils.waitUntilTrue(() => groupDirExists(new ZKGroupDirs(otherGroup)),
      "DeleteConsumerGroupInZK should not delete unrelated consumer group directories")
  }

  @Test
  def testGroupWideDeleteInZKDoesNothingForActiveConsumerGroup(): Unit = {
    val topic = "test"
    val groupToDelete = "groupToDelete"
    val otherGroup = "otherGroup"

    createTopic(topic, 1, 3)
    fillInConsumerGroupInfo(topic, groupToDelete, "consumer", 0, 10, true)
    fillInConsumerGroupInfo(topic, otherGroup, "consumer", 0, 10, false)

    AdminUtils.deleteConsumerGroupInZK(zkUtils, groupToDelete)

    TestUtils.waitUntilTrue(() => groupDirExists(new ZKGroupDirs(groupToDelete)),
      "DeleteConsumerGroupInZK should not delete the provided consumer group's directory if the consumer group is still active")
    TestUtils.waitUntilTrue(() => groupDirExists(new ZKGroupDirs(otherGroup)),
      "DeleteConsumerGroupInZK should not delete unrelated consumer group directories")
  }

  @Test
  def testGroupTopicWideDeleteInZKForGroupConsumingOneTopic(): Unit = {
    val topic = "test"
    val groupToDelete = "groupToDelete"
    val otherGroup = "otherGroup"
    createTopic(topic, 1, 3)
    fillInConsumerGroupInfo(topic, groupToDelete, "consumer", 0, 10, false)
    fillInConsumerGroupInfo(topic, otherGroup, "consumer", 0, 10, false)

    AdminUtils.deleteConsumerGroupInfoForTopicInZK(zkUtils, groupToDelete, topic)

    TestUtils.waitUntilTrue(() => !groupDirExists(new ZKGroupDirs(groupToDelete)),
      "DeleteConsumerGroupInfoForTopicInZK should delete the provided consumer group's directory if it just consumes from one topic")
    TestUtils.waitUntilTrue(() => groupTopicOffsetAndOwnerDirsExist(new ZKGroupTopicDirs(otherGroup, topic)),
      "DeleteConsumerGroupInfoForTopicInZK should not delete unrelated consumer group owner and offset directories")
  }

  @Test
  def testGroupTopicWideDeleteInZKForGroupConsumingMultipleTopics(): Unit = {
    val topicToDelete = "topicToDelete"
    val otherTopic = "otherTopic"
    val groupToDelete = "groupToDelete"
    val otherGroup = "otherGroup"
    createTopic(topicToDelete, 1, 3)
    createTopic(otherTopic, 1, 3)

    fillInConsumerGroupInfo(topicToDelete, groupToDelete, "consumer", 0, 10, false)
    fillInConsumerGroupInfo(otherTopic, groupToDelete, "consumer", 0, 10, false)
    fillInConsumerGroupInfo(topicToDelete, otherGroup, "consumer", 0, 10, false)

    AdminUtils.deleteConsumerGroupInfoForTopicInZK(zkUtils, groupToDelete, topicToDelete)

    TestUtils.waitUntilTrue(() => !groupTopicOffsetAndOwnerDirsExist(new ZKGroupTopicDirs(groupToDelete, topicToDelete)),
      "DeleteConsumerGroupInfoForTopicInZK should delete the provided consumer group's owner and offset directories for the given topic")
    TestUtils.waitUntilTrue(() => groupTopicOffsetAndOwnerDirsExist(new ZKGroupTopicDirs(groupToDelete, otherTopic)),
      "DeleteConsumerGroupInfoForTopicInZK should not delete the provided consumer group's owner and offset directories for unrelated topics")
    TestUtils.waitUntilTrue(() => groupTopicOffsetAndOwnerDirsExist(new ZKGroupTopicDirs(otherGroup, topicToDelete)),
      "DeleteConsumerGroupInfoForTopicInZK should not delete unrelated consumer group owner and offset directories")
  }

  @Test
  def testGroupTopicWideDeleteInZKDoesNothingForActiveGroupConsumingMultipleTopics(): Unit = {
    val topicToDelete = "topicToDelete"
    val otherTopic = "otherTopic"
    val group = "group"
    createTopic(topicToDelete, 1, 3)
    createTopic(otherTopic, 1, 3)

    fillInConsumerGroupInfo(topicToDelete, group, "consumer", 0, 10, true)
    fillInConsumerGroupInfo(otherTopic, group, "consumer", 0, 10, true)

    AdminUtils.deleteConsumerGroupInfoForTopicInZK(zkUtils, group, topicToDelete)

    TestUtils.waitUntilTrue(() => groupTopicOffsetAndOwnerDirsExist(new ZKGroupTopicDirs(group, topicToDelete)),
      "DeleteConsumerGroupInfoForTopicInZK should not delete the provided consumer group's owner and offset directories for the given topic if the consumer group is still active")
    TestUtils.waitUntilTrue(() => groupTopicOffsetAndOwnerDirsExist(new ZKGroupTopicDirs(group, otherTopic)),
      "DeleteConsumerGroupInfoForTopicInZK should not delete the provided consumer group's owner and offset directories for unrelated topics")
  }

  @Test
  def testTopicWideDeleteInZK(): Unit = {
    val topicToDelete = "topicToDelete"
    val otherTopic = "otherTopic"
    val groups = Seq("group1", "group2")

    createTopic(topicToDelete, 1, 3)
    createTopic(otherTopic, 1, 3)
    val groupTopicDirsForTopicToDelete = groups.map(group => new ZKGroupTopicDirs(group, topicToDelete))
    val groupTopicDirsForOtherTopic = groups.map(group => new ZKGroupTopicDirs(group, otherTopic))
    groupTopicDirsForTopicToDelete.foreach(dir => fillInConsumerGroupInfo(topicToDelete, dir.group, "consumer", 0, 10, false))
    groupTopicDirsForOtherTopic.foreach(dir => fillInConsumerGroupInfo(otherTopic, dir.group, "consumer", 0, 10, false))

    AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topicToDelete)

    TestUtils.waitUntilTrue(() => !groupTopicDirsForTopicToDelete.exists(groupTopicOffsetAndOwnerDirsExist),
      "Consumer group info on deleted topic should be deleted by DeleteAllConsumerGroupInfoForTopicInZK")
    TestUtils.waitUntilTrue(() => groupTopicDirsForOtherTopic.forall(groupTopicOffsetAndOwnerDirsExist),
      "Consumer group info on unrelated topics should not be deleted by DeleteAllConsumerGroupInfoForTopicInZK")
  }

  @Test
  def testConsumptionOnRecreatedTopicAfterTopicWideDeleteInZK(): Unit = {
    val topic = "topic"
    val group = "group"

    createTopic(topic, 1, 3)
    val dir = new ZKGroupTopicDirs(group, topic)
    fillInConsumerGroupInfo(topic, dir.group, "consumer", 0, 10, false)

    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic)

    TestUtils.waitUntilTrue(() => !groupDirExists(dir),
      "Consumer group info on related topics should be deleted by DeleteAllConsumerGroupInfoForTopicInZK")
    //produce events
    val producer = TestUtils.createNewProducer(brokerList)
    try {
      produceEvents(producer, topic, List.fill(10)("test"))
    } finally {
      producer.close()
    }

    //consume events
    val consumerProps = TestUtils.createConsumerProperties(zkConnect, group, "consumer")
    consumerProps.put("auto.commit.enable", "false")
    consumerProps.put("auto.offset.reset", "smallest")
    consumerProps.put("consumer.timeout.ms", "2000")
    consumerProps.put("fetch.wait.max.ms", "0")
    val consumerConfig = new ConsumerConfig(consumerProps)
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    try {
      val messageStream = consumerConnector.createMessageStreams(Map(topic -> 1))(topic).head
      consumeEvents(messageStream, 5)
      consumerConnector.commitOffsets(false)
    } finally {
      consumerConnector.shutdown()
    }

    TestUtils.waitUntilTrue(() => groupTopicOffsetAndOwnerDirsExist(dir),
      "Consumer group info should exist after consuming from a recreated topic")
  }

  private def fillInConsumerGroupInfo(topic: String, group: String, consumerId: String, partition: Int, offset: Int, registerConsumer: Boolean): Unit = {
    val consumerProps = TestUtils.createConsumerProperties(zkConnect, group, consumerId)
    val consumerConfig = new ConsumerConfig(consumerProps)
    val dir = new ZKGroupTopicDirs(group, topic)
    TestUtils.updateConsumerOffset(consumerConfig, dir.consumerOffsetDir + "/" + partition, offset)
    zkUtils.createEphemeralPathExpectConflict(zkUtils.getConsumerPartitionOwnerPath(group, topic, partition), "")
    zkUtils.makeSurePersistentPathExists(dir.consumerRegistryDir)
    if (registerConsumer) {
      zkUtils.createEphemeralPathExpectConflict(dir.consumerRegistryDir + "/" + consumerId, "")
    }
  }

  private def groupDirExists(dir: ZKGroupDirs) = {
    zkUtils.pathExists(dir.consumerGroupDir)
  }

  private def groupTopicOffsetAndOwnerDirsExist(dir: ZKGroupTopicDirs) = {
    zkUtils.pathExists(dir.consumerOffsetDir) && zkUtils.pathExists(dir.consumerOwnerDir)
  }

  private def produceEvents(producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String, messages: List[String]): Unit = {
    messages.foreach(message => producer.send(new ProducerRecord(topic, message.getBytes(StandardCharsets.UTF_8))))
  }

  private def consumeEvents(messageStream: KafkaStream[Array[Byte], Array[Byte]], n: Int): Unit = {
    val iter = messageStream.iterator
    (0 until n).foreach(_ => iter.next)
  }
}
