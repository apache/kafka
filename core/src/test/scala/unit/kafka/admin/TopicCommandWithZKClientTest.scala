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
import kafka.utils.{Exit, Logging, TestUtils}
import kafka.zk.{ConfigEntityChangeNotificationZNode, DeleteTopicsTopicZNode, ZooKeeperTestHarness}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.errors.{InvalidPartitionsException, InvalidReplicationFactorException, TopicExistsException}
import org.apache.kafka.common.internals.Topic
import org.junit.Assert._
import org.junit.rules.TestName
import org.junit.{After, Before, Rule, Test}
import org.scalatest.Assertions.intercept

import scala.util.Random

class TopicCommandWithZKClientTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  private var topicService: ZookeeperTopicService = _
  private var testTopicName: String = _

  private val _testName = new TestName
  @Rule def testName = _testName

  @Before
  def setup(): Unit = {
    topicService = ZookeeperTopicService(zkClient)
    testTopicName = s"${testName.getMethodName}-${Random.alphanumeric.take(10).mkString}"
  }

  @After
  def teardown(): Unit = {
    if (topicService != null)
      topicService.close()
  }

  @Test
  def testCreate(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "1", "--topic", testTopicName)))

    assertTrue(zkClient.getAllTopicsInCluster().contains(testTopicName))
  }

  @Test
  def testCreateWithConfigs(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName)
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", configResource.name(), "--config", "delete.retention.ms=1000")))

    val configs = zkClient.getEntityConfigs(ConfigType.Topic, testTopicName)
    assertEquals(1000, Integer.valueOf(configs.getProperty("delete.retention.ms")))
  }

  @Test
  def testCreateIfNotExists(): Unit = {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val numPartitions = 1

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", testTopicName))
    topicService.createTopic(createOpts)

    // try to re-create the topic without --if-not-exists
    intercept[TopicExistsException] {
      topicService.createTopic(createOpts)
    }

    // try to re-create the topic with --if-not-exists
    val createNotExistsOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", testTopicName, "--if-not-exists"))
    topicService.createTopic(createNotExistsOpts)
  }

  @Test
  def testCreateWithReplicaAssignment(): Unit = {
    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--replica-assignment", "5:4,3:2,1:0", "--topic", testTopicName))
    topicService.createTopic(createOpts)

    val replicas0 = zkClient.getReplicasForPartition(new TopicPartition(testTopicName, 0))
    assertEquals(List(5, 4), replicas0)
    val replicas1 = zkClient.getReplicasForPartition(new TopicPartition(testTopicName, 1))
    assertEquals(List(3, 2), replicas1)
    val replicas2 = zkClient.getReplicasForPartition(new TopicPartition(testTopicName, 2))
    assertEquals(List(1, 0), replicas2)
  }

  @Test
  def testCreateWithInvalidReplicationFactor(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    intercept[InvalidReplicationFactorException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", (Short.MaxValue+1).toString, "--topic", testTopicName)))
    }
  }

  @Test
  def testCreateWithNegativeReplicationFactor(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    intercept[InvalidReplicationFactorException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", "-1", "--topic", testTopicName)))
    }
  }

  @Test
  def testCreateWithNegativePartitionCount(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    intercept[InvalidPartitionsException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--replication-factor", "1", "--topic", testTopicName)))
    }
  }

  @Test
  def testInvalidTopicLevelConfig(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val createOpts = new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName,
        "--config", "message.timestamp.type=boom"))
    intercept[ConfigException] {
      topicService.createTopic(createOpts)
    }

    // try to create the topic with another invalid config
    val createOpts2 = new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName,
        "--config", "message.format.version=boom"))
    intercept[ConfigException] {
      topicService.createTopic(createOpts2)
    }
  }

  @Test
  def testListTopics(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array())))

    assertTrue(output.contains(testTopicName))
  }

  @Test
  def testListTopicsWithIncludeList(): Unit = {
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

    adminZkClient.createTopic(testTopicName, 2, 2)
    adminZkClient.createTopic(Topic.GROUP_METADATA_TOPIC_NAME, 2, 2)

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--exclude-internal"))))

    assertTrue(output.contains(testTopicName))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }

  @Test
  def testAlterPartitionCount(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    adminZkClient.createTopic(testTopicName, 2, 2)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--partitions", "3")))

    assertEquals(3, zkClient.getPartitionsForTopics(Set(testTopicName))(testTopicName).size)
  }

  @Test
  def testAlterAssignment(): Unit = {
    val brokers = List(0, 1, 2, 3, 4, 5)
    TestUtils.createBrokersInZk(zkClient, brokers)

    adminZkClient.createTopic(testTopicName, 2, 2)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3")))

    val replicas0 = zkClient.getReplicasForPartition(new TopicPartition(testTopicName, 2))
    assertEquals(3, zkClient.getPartitionsForTopics(Set(testTopicName))(testTopicName).size)
    assertEquals(List(4,2), replicas0)
  }

  @Test
  def testAlterWithInvalidPartitionCount(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))

    intercept[InvalidPartitionsException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--topic", testTopicName)))
    }
  }

  @Test
  def testAlterIfExists(): Unit = {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // alter a topic that does not exist without --if-exists
    val alterOpts = new TopicCommandOptions(Array("--topic", testTopicName, "--partitions", "1"))
    intercept[IllegalArgumentException] {
      topicService.alterTopic(alterOpts)
    }

    // alter a topic that does not exist with --if-exists
    val alterExistsOpts = new TopicCommandOptions(Array("--topic", testTopicName, "--partitions", "1", "--if-exists"))
    topicService.alterTopic(alterExistsOpts)
  }

  @Test
  def testAlterConfigs(): Unit = {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--config", "cleanup.policy=compact")))

    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    assertTrue("The output should contain the modified config", output.contains("Configs: cleanup.policy=compact"))

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--config", "cleanup.policy=delete")))

    val output2 = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    assertTrue("The output should contain the modified config", output2.contains("Configs: cleanup.policy=delete"))
  }

  @Test
  def testConfigPreservationAcrossPartitionAlteration(): Unit = {
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
      "--topic", testTopicName))
    topicService.createTopic(createOpts)
    val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString, "--topic", testTopicName))
    topicService.alterTopic(alterOpts)
    val newProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
  }

  @Test
  def testTopicDeletion(): Unit = {

    val numPartitionsOriginal = 1

    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // create the NormalTopic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", testTopicName))
    topicService.createTopic(createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", testTopicName))
    val deletePath = DeleteTopicsTopicZNode.path(testTopicName)
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
  def testDeleteIfExists(): Unit = {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // delete a topic that does not exist without --if-exists
    val deleteOpts = new TopicCommandOptions(Array("--topic", testTopicName))
    intercept[IllegalArgumentException] {
      topicService.deleteTopic(deleteOpts)
    }

    // delete a topic that does not exist with --if-exists
    val deleteExistsOpts = new TopicCommandOptions(Array("--topic", testTopicName, "--if-exists"))
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
    val deleteOffsetTopicPath = DeleteTopicsTopicZNode.path(Topic.GROUP_METADATA_TOPIC_NAME)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deleteOffsetTopicPath))
    intercept[AdminOperationException] {
      topicService.deleteTopic(deleteOffsetTopicOpts)
    }
  }

  @Test
  def testDescribeIfTopicNotExists(): Unit = {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // describe topic that does not exist
    val describeOpts = new TopicCommandOptions(Array("--topic", testTopicName))
    intercept[IllegalArgumentException] {
      topicService.describeTopic(describeOpts)
    }

    // describe all topics
    val describeOptsAllTopics = new TopicCommandOptions(Array())
    // should not throw any error
    topicService.describeTopic(describeOptsAllTopics)

    // describe topic that does not exist with --if-exists
    val describeOptsWithExists = new TopicCommandOptions(Array("--topic", testTopicName, "--if-exists"))
    // should not throw any error
    topicService.describeTopic(describeOptsWithExists)
  }

  @Test
  def testCreateAlterTopicWithRackAware(): Unit = {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkClient)

    val numPartitions = 18
    val replicationFactor = 3
    val createOpts = new TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--topic", testTopicName))
    topicService.createTopic(createOpts)

    var assignment = zkClient.getReplicaAssignmentForTopics(Set(testTopicName)).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)

    val alteredNumPartitions = 36
    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", alteredNumPartitions.toString,
      "--topic", testTopicName))
    topicService.alterTopic(alterOpts)
    assignment = zkClient.getReplicaAssignmentForTopics(Set(testTopicName)).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor)
  }

  @Test
  def testDescribe(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    adminZkClient.createTopic(testTopicName, 2, 2)
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    val rows = output.split("\n")
    assertEquals(3, rows.size)
    rows(0).startsWith("Topic:testTopic\tPartitionCount:2")
  }

  @Test
  def testDescribeReportOverriddenConfigs(): Unit = {
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val config = "file.delete.delay.ms=1000"
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName)
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", configResource.name(), "--config", config)))
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array())))
    assertTrue(output.contains(config))
  }

  @Test
  def testDescribeAndListTopicsMarkedForDeletion(): Unit = {
    val brokers = List(0)
    val markedForDeletionDescribe = "MarkedForDeletion"
    val markedForDeletionList = "marked for deletion"
    TestUtils.createBrokersInZk(zkClient, brokers)

    val createOpts = new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName))
    topicService.createTopic(createOpts)

    // delete the broker first, so when we attempt to delete the topic it gets into "marked for deletion"
    TestUtils.deleteBrokersInZk(zkClient, brokers)
    topicService.deleteTopic(new TopicCommandOptions(Array("--topic", testTopicName)))

    // Test describe topics
    def describeTopicsWithConfig(): Unit = {
      topicService.describeTopic(new TopicCommandOptions(Array("--describe")))
    }
    val outputWithConfig = TestUtils.grabConsoleOutput(describeTopicsWithConfig())
    assertTrue(outputWithConfig.contains(testTopicName) && outputWithConfig.contains(markedForDeletionDescribe))

    def describeTopicsNoConfig(): Unit = {
      topicService.describeTopic(new TopicCommandOptions(Array("--describe", "--unavailable-partitions")))
    }
    val outputNoConfig = TestUtils.grabConsoleOutput(describeTopicsNoConfig())
    assertTrue(outputNoConfig.contains(testTopicName) && outputNoConfig.contains(markedForDeletionDescribe))

    // Test list topics
    def listTopics(): Unit = {
      topicService.listTopics(new TopicCommandOptions(Array("--list")))
    }
    val output = TestUtils.grabConsoleOutput(listTopics())
    assertTrue(output.contains(testTopicName) && output.contains(markedForDeletionList))
  }

  @Test
  def testDescribeAndListTopicsWithoutInternalTopics(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    topicService.createTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))
    // create a internal topic
    topicService.createTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", Topic.GROUP_METADATA_TOPIC_NAME)))

    // test describe
    var output = TestUtils.grabConsoleOutput(topicService.describeTopic(
      new TopicCommandOptions(Array("--describe", "--exclude-internal"))))
    assertTrue(output.contains(testTopicName))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))

    // test list
    output = TestUtils.grabConsoleOutput(topicService.listTopics(
      new TopicCommandOptions(Array("--list", "--exclude-internal"))))
    assertTrue(output.contains(testTopicName))
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

  @Test
  def testAlterInternalTopicPartitionCount(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)
    
    // create internal topics
    adminZkClient.createTopic(Topic.GROUP_METADATA_TOPIC_NAME, 1, 1)
    adminZkClient.createTopic(Topic.TRANSACTION_STATE_TOPIC_NAME, 1, 1)

    def expectAlterInternalTopicPartitionCountFailed(topic: String): Unit = {
      try {
        topicService.alterTopic(new TopicCommandOptions(
          Array("--topic", topic, "--partitions", "2")))
        fail("Should have thrown an IllegalArgumentException")
      } catch {
        case _: IllegalArgumentException => // expected
      }
    }
    expectAlterInternalTopicPartitionCountFailed(Topic.GROUP_METADATA_TOPIC_NAME)
    expectAlterInternalTopicPartitionCountFailed(Topic.TRANSACTION_STATE_TOPIC_NAME)
  }

  @Test
  def testCreateWithUnspecifiedReplicationFactorAndPartitionsWithZkClient(): Unit = {
    assertExitCode(1, () =>
      new TopicCommandOptions(Array("--create", "--zookeeper", "zk", "--topic", testTopicName)).checkArgs()
    )
  }

  def assertExitCode(expected: Int, method: () => Unit): Unit = {
    def mockExitProcedure(exitCode: Int, exitMessage: Option[String]): Nothing = {
      assertEquals(expected, exitCode)
      throw new RuntimeException
    }
    Exit.setExitProcedure(mockExitProcedure)
    try {
      intercept[RuntimeException] {
        method()
      }
    } finally {
      Exit.resetExitProcedure()
    }
  }
}
