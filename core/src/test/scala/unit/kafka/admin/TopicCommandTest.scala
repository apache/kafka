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

import kafka.admin.TopicCommand.{TopicCommandOptions, ZookeeperTopicService}
import kafka.server.ConfigType
import kafka.utils.ZkUtils.getDeleteTopicPath
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{ConfigEntityChangeNotificationZNode, DeleteTopicsTopicZNode, ZooKeeperTestHarness}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.errors.{InvalidPartitionsException, InvalidReplicationFactorException, TopicExistsException}
import org.apache.kafka.common.internals.Topic
import org.junit.Assert._
import org.junit.{After, Before, Test}

class TopicCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  private var topicService: ZookeeperTopicService = _

  @Before
  def setup(): Unit = {
    topicService = ZookeeperTopicService(zkClient)
  }

  @After
  def teardown(): Unit = {
    if (topicService != null)
      topicService.close()
  }

  @Test
  def testCreate(): Unit = {
    val topic = "test"

    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "1", "--topic", topic)))

    assertTrue(zkClient.getAllTopicsInCluster.contains(topic))
  }

  @Test
  def testCreateWithConfigs(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic")
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", configResource.name(), "--config", "delete.retention.ms=1000")))

    val configs = zkClient.getEntityConfigs(ConfigType.Topic, "testTopic")
    assertEquals(1000, Integer.valueOf(configs.getProperty("delete.retention.ms")))
  }

  @Test
  def testCreateIfNotExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topic = "test"
    val numPartitions = 1

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", topic))
    topicService.createTopic(createOpts)

    // try to re-create the topic without --if-not-exists
    intercept[TopicExistsException] {
      topicService.createTopic(createOpts)
    }

    // try to re-create the topic with --if-not-exists
    val createNotExistsOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", topic, "--if-not-exists"))
    topicService.createTopic(createNotExistsOpts)
  }

  @Test
  def testCreateWithReplicaAssignment(): Unit = {
    val topic = "test"

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--replica-assignment", "5:4,3:2,1:0", "--topic", topic))
    topicService.createTopic(createOpts)

    val replicas0 = zkClient.getReplicasForPartition(new TopicPartition(topic, 0))
    assertEquals(List(5, 4), replicas0)
    val replicas1 = zkClient.getReplicasForPartition(new TopicPartition(topic, 1))
    assertEquals(List(3, 2), replicas1)
    val replicas2 = zkClient.getReplicasForPartition(new TopicPartition(topic, 2))
    assertEquals(List(1, 0), replicas2)
  }

  @Test
  def testCreateWithInvalidReplicationFactor() {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    intercept[InvalidReplicationFactorException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", (Short.MaxValue+1).toString, "--topic", "testTopic")))
    }
  }

  @Test
  def testCreateWithNegativeReplicationFactor(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    intercept[InvalidReplicationFactorException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", "-1", "--topic", "testTopic")))
    }
  }

  @Test
  def testCreateWithNegativePartitionCount(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    intercept[InvalidPartitionsException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--replication-factor", "1", "--topic", "testTopic")))
    }
  }

  @Test
  def testInvalidTopicLevelConfig(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val createOpts = new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", "test",
        "--config", "message.timestamp.type=boom"))
    intercept[ConfigException] {
      topicService.createTopic(createOpts)
    }

    // try to create the topic with another invalid config
    val createOpts2 = new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", "test",
        "--config", "message.format.version=boom"))
    intercept[ConfigException] {
      topicService.createTopic(createOpts2)
    }
  }

  @Test
  def testListTopics(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topic = "testTopic"
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", topic)))

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array())))

    assertTrue(output.contains(topic))
  }

  @Test
  def testListTopicsWithWhitelist(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topic1 = "kafka.testTopic1"
    val topic2 = "kafka.testTopic2"
    val topic3 = "oooof.testTopic1"
    adminZkClient.createTopic(topic1, 2, 2)
    adminZkClient.createTopic(topic2, 2, 2)
    adminZkClient.createTopic(topic3, 2, 2)

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--topic", "kafka.*"))))

    assertTrue(output.contains(topic1))
    assertTrue(output.contains(topic2))
    assertFalse(output.contains(topic3))
  }

  @Test
  def testListTopicsWithExcludeInternal(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topic1 = "kafka.testTopic1"
    adminZkClient.createTopic(topic1, 2, 2)
    adminZkClient.createTopic(Topic.GROUP_METADATA_TOPIC_NAME, 2, 2)

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--exclude-internal"))))

    assertTrue(output.contains(topic1))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }

  @Test
  def testAlterPartitionCount(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topic1 = "testTopic1"

    adminZkClient.createTopic(topic1, 2, 2)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", topic1, "--partitions", "3")))

    assertEquals(3, zkClient.getPartitionsForTopics(Set(topic1))(topic1).size)
  }

  @Test
  def testAlterAssignment(): Unit = {
    val brokers = List(0, 1, 2, 3, 4, 5)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topic1 = "testTopic1"

    adminZkClient.createTopic(topic1, 2, 2)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", topic1, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3")))

    val replicas0 = zkClient.getReplicasForPartition(new TopicPartition(topic1, 2))
    assertEquals(3, zkClient.getPartitionsForTopics(Set(topic1))(topic1).size)
    assertEquals(List(4,2), replicas0)
  }

  @Test
  def testAlterWithInvalidPartitionCount(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", "testTopic")))

    intercept[InvalidPartitionsException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--topic", "testTopic")))
    }
  }

  @Test
  def testAlterIfExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // alter a topic that does not exist without --if-exists
    val alterOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1"))
    intercept[IllegalArgumentException] {
      topicService.alterTopic(alterOpts)
    }

    // alter a topic that does not exist with --if-exists
    val alterExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1", "--if-exists"))
    topicService.alterTopic(alterExistsOpts)
  }

  @Test
  def testConfigPreservationAcrossPartitionAlteration() {
    val topic = "test"
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)
    // create the topic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", topic))
    topicService.createTopic(createOpts)
    val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString, "--topic", topic))
    topicService.alterTopic(alterOpts)
    val newProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
  }

  @Test
  def testTopicDeletion() {

    val normalTopic = "test"

    val numPartitionsOriginal = 1

    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // create the NormalTopic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", normalTopic))
    topicService.createTopic(createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", normalTopic))
    val deletePath = DeleteTopicsTopicZNode.path(normalTopic)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deletePath))
    topicService.deleteTopic(deleteOpts)
    assertTrue("Delete path for topic should exist after deletion.", zkClient.pathExists(deletePath))

    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    topicService.createTopic(createOffsetTopicOpts)

    // try to delete the Topic.GROUP_METADATA_TOPIC_NAME and make sure it doesn't
    val deleteOffsetTopicOpts = new TopicCommandOptions(Array("--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    val deleteOffsetTopicPath = DeleteTopicsTopicZNode.path(Topic.GROUP_METADATA_TOPIC_NAME)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deleteOffsetTopicPath))
    intercept[AdminOperationException] {
      topicService.deleteTopic(deleteOffsetTopicOpts)
    }
    assertFalse("Delete path for topic shouldn't exist after deletion.", zkClient.pathExists(deleteOffsetTopicPath))
  }

  @Test
  def testDeleteIfExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // delete a topic that does not exist without --if-exists
    val deleteOpts = new TopicCommandOptions(Array("--topic", "test"))
    intercept[IllegalArgumentException] {
      topicService.deleteTopic(deleteOpts)
    }

    // delete a topic that does not exist with --if-exists
    val deleteExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--if-exists"))
    topicService.deleteTopic(deleteExistsOpts)
  }

  @Test
  def testDeleteInternalTopic(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", "1",
      "--replication-factor", "1",
      "--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    topicService.createTopic(createOffsetTopicOpts)

    val deleteOffsetTopicOpts = new TopicCommandOptions(Array("--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    val deleteOffsetTopicPath = getDeleteTopicPath(Topic.GROUP_METADATA_TOPIC_NAME)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deleteOffsetTopicPath))
    intercept[AdminOperationException] {
      topicService.deleteTopic(deleteOffsetTopicOpts)
    }
  }

  @Test
  def testDescribeIfTopicNotExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // describe topic that does not exist
    val describeOpts = new TopicCommandOptions(Array("--topic", "test"))
    intercept[IllegalArgumentException] {
      TopicCommand.describeTopic(zkClient, describeOpts)
    }

    // describe topic that does not exist with --if-exists
    val describeOptsWithExists = new TopicCommandOptions(Array("--topic", "test", "--if-exists"))
    // should not throw any error
    TopicCommand.describeTopic(zkClient, describeOptsWithExists)
  }

  @Test
  def testCreateAlterTopicWithRackAware() {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkClient)

    val numPartitions = 18
    val replicationFactor = 3
    val createOpts = new TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--topic", "foo"))
    topicService.createTopic(createOpts)

    var assignment = zkClient.getReplicaAssignmentForTopics(Set("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)

    val alteredNumPartitions = 36
    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", alteredNumPartitions.toString,
      "--topic", "foo"))
    topicService.alterTopic(alterOpts)
    assignment = zkClient.getReplicaAssignmentForTopics(Set("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor)
  }

  @Test
  def testDescribe(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topic = "testTopic"
    adminZkClient.createTopic(topic, 2, 2)
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", topic))))
    val rows = output.split("\n")
    assertEquals(3, rows.size)
    rows(0).startsWith("Topic:testTopic\tPartitionCount:2")
  }

  @Test
  def testDescribeReportOverriddenConfigs(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val config = "file.delete.delay.ms=1000"
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic")
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", configResource.name(), "--config", config)))
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array())))
    assertTrue(output.contains(config))
  }

  @Test
  def testDescribeAndListTopicsMarkedForDeletion() {
    val brokers = List(0)
    val topic = "testtopic"
    val markedForDeletionDescribe = "MarkedForDeletion"
    val markedForDeletionList = "marked for deletion"
    TestUtils.createBrokersInZk(zkClient, brokers)

    val createOpts = new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", topic))
    topicService.createTopic(createOpts)

    // delete the broker first, so when we attempt to delete the topic it gets into "marked for deletion"
    TestUtils.deleteBrokersInZk(zkClient, brokers)
    topicService.deleteTopic(new TopicCommandOptions(Array("--topic", topic)))

    // Test describe topics
    def describeTopicsWithConfig() {
      topicService.describeTopic(new TopicCommandOptions(Array("--describe")))
    }
    val outputWithConfig = TestUtils.grabConsoleOutput(describeTopicsWithConfig())
    assertTrue(outputWithConfig.contains(topic) && outputWithConfig.contains(markedForDeletionDescribe))

    def describeTopicsNoConfig() {
      topicService.describeTopic(new TopicCommandOptions(Array("--describe", "--unavailable-partitions")))
    }
    val outputNoConfig = TestUtils.grabConsoleOutput(describeTopicsNoConfig())
    assertTrue(outputNoConfig.contains(topic) && outputNoConfig.contains(markedForDeletionDescribe))

    // Test list topics
    def listTopics() {
      topicService.listTopics(new TopicCommandOptions(Array("--list")))
    }
    val output = TestUtils.grabConsoleOutput(listTopics())
    assertTrue(output.contains(topic) && output.contains(markedForDeletionList))
  }

  @Test
  def testDescribeAndListTopicsWithoutInternalTopics() {
    val brokers = List(0)
    val topic = "testDescribeAndListTopicsWithoutInternalTopics"
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", topic)))
    // create a internal topic
    topicService.createTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", Topic.GROUP_METADATA_TOPIC_NAME)))

    // test describe
    var output = TestUtils.grabConsoleOutput(topicService.describeTopic(
      new TopicCommandOptions(Array("--describe", "--exclude-internal"))))
    assertTrue(output.contains(topic))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))

    // test list
    output = TestUtils.grabConsoleOutput(topicService.listTopics(
      new TopicCommandOptions(Array("--list", "--exclude-internal"))))
    assertTrue(output.contains(topic))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }

  @Test
  def testTopicOperationsWithRegexSymbolInTopicName(): Unit = {
    val topic1 = "test.topic"
    val topic2 = "test-topic"
    val escapedTopic = "\"test\\.topic\""
    val unescapedTopic = "test.topic"
    val numPartitionsOriginal = 1

    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // create the topics
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1", "--topic", topic1))
    topicService.createTopic(createOpts)
    val createOpts2 = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1", "--topic", topic2))
    topicService.createTopic(createOpts2)

    val escapedCommandOpts = new TopicCommandOptions(Array("--topic", escapedTopic))
    val unescapedCommandOpts = new TopicCommandOptions(Array("--topic", unescapedTopic))

    // topic actions with escaped regex do not affect 'test-topic'
    // topic actions with unescaped topic affect 'test-topic'

    assertFalse(TestUtils.grabConsoleOutput(topicService.describeTopic(escapedCommandOpts)).contains(topic2))
    assertTrue(TestUtils.grabConsoleOutput(topicService.describeTopic(unescapedCommandOpts)).contains(topic2))

    assertFalse(TestUtils.grabConsoleOutput(topicService.deleteTopic(escapedCommandOpts)).contains(topic2))
    assertTrue(TestUtils.grabConsoleOutput(topicService.deleteTopic(unescapedCommandOpts)).contains(topic2))
  }
}