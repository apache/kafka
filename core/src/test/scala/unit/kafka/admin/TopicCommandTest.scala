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

import org.junit.Assert._
import org.junit.Test
import kafka.utils.Logging
import kafka.utils.TestUtils
import kafka.zk.{ConfigEntityChangeNotificationZNode, ZooKeeperTestHarness}
import kafka.server.ConfigType
import kafka.admin.TopicCommand.TopicCommandOptions
import kafka.utils.ZkUtils.getDeleteTopicPath
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.internals.Topic

class TopicCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

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
    TopicCommand.createTopic(zkClient, createOpts)
    val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString, "--topic", topic))
    TopicCommand.alterTopic(zkClient, alterOpts)
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
    TopicCommand.createTopic(zkClient, createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", normalTopic))
    val deletePath = getDeleteTopicPath(normalTopic)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deletePath))
    TopicCommand.deleteTopic(zkClient, deleteOpts)
    assertTrue("Delete path for topic should exist after deletion.", zkClient.pathExists(deletePath))

    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    TopicCommand.createTopic(zkClient, createOffsetTopicOpts)

    // try to delete the Topic.GROUP_METADATA_TOPIC_NAME and make sure it doesn't
    val deleteOffsetTopicOpts = new TopicCommandOptions(Array("--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    val deleteOffsetTopicPath = getDeleteTopicPath(Topic.GROUP_METADATA_TOPIC_NAME)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deleteOffsetTopicPath))
    intercept[AdminOperationException] {
      TopicCommand.deleteTopic(zkClient, deleteOffsetTopicOpts)
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
      TopicCommand.deleteTopic(zkClient, deleteOpts)
    }

    // delete a topic that does not exist with --if-exists
    val deleteExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--if-exists"))
    TopicCommand.deleteTopic(zkClient, deleteExistsOpts)
  }

  @Test
  def testAlterIfExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // alter a topic that does not exist without --if-exists
    val alterOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1"))
    intercept[IllegalArgumentException] {
      TopicCommand.alterTopic(zkClient, alterOpts)
    }

    // alter a topic that does not exist with --if-exists
    val alterExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1", "--if-exists"))
    TopicCommand.alterTopic(zkClient, alterExistsOpts)
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
    TopicCommand.createTopic(zkClient, createOpts)

    // try to re-create the topic without --if-not-exists
    intercept[TopicExistsException] {
      TopicCommand.createTopic(zkClient, createOpts)
    }

    // try to re-create the topic with --if-not-exists
    val createNotExistsOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", topic, "--if-not-exists"))
    TopicCommand.createTopic(zkClient, createNotExistsOpts)
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
    TopicCommand.createTopic(zkClient, createOpts)

    var assignment = zkClient.getReplicaAssignmentForTopics(Set("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)

    val alteredNumPartitions = 36
    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", alteredNumPartitions.toString,
      "--topic", "foo"))
    TopicCommand.alterTopic(zkClient, alterOpts)
    assignment = zkClient.getReplicaAssignmentForTopics(Set("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor)
  }

  @Test
  def testDescribeAndListTopicsMarkedForDeletion() {
    val brokers = List(0)
    val topic = "testtopic"
    val markedForDeletionDescribe = "MarkedForDeletion"
    val markedForDeletionList = "marked for deletion"
    TestUtils.createBrokersInZk(zkClient, brokers)

    val createOpts = new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", topic))
    TopicCommand.createTopic(zkClient, createOpts)

    // delete the broker first, so when we attempt to delete the topic it gets into "marked for deletion"
    TestUtils.deleteBrokersInZk(zkClient, brokers)
    TopicCommand.deleteTopic(zkClient, new TopicCommandOptions(Array("--topic", topic)))

    // Test describe topics
    def describeTopicsWithConfig() {
      TopicCommand.describeTopic(zkClient, new TopicCommandOptions(Array("--describe")))
    }
    val outputWithConfig = TestUtils.grabConsoleOutput(describeTopicsWithConfig)
    assertTrue(outputWithConfig.contains(topic) && outputWithConfig.contains(markedForDeletionDescribe))

    def describeTopicsNoConfig() {
      TopicCommand.describeTopic(zkClient, new TopicCommandOptions(Array("--describe", "--unavailable-partitions")))
    }
    val outputNoConfig = TestUtils.grabConsoleOutput(describeTopicsNoConfig)
    assertTrue(outputNoConfig.contains(topic) && outputNoConfig.contains(markedForDeletionDescribe))

    // Test list topics
    def listTopics() {
      TopicCommand.listTopics(zkClient, new TopicCommandOptions(Array("--list")))
    }
    val output = TestUtils.grabConsoleOutput(listTopics)
    assertTrue(output.contains(topic) && output.contains(markedForDeletionList))
  }

  @Test
  def testInvalidTopicLevelConfig(): Unit = {
    val brokers = List(0)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // create the topic
    try {
      val createOpts = new TopicCommandOptions(
        Array("--partitions", "1", "--replication-factor", "1", "--topic", "test",
          "--config", "message.timestamp.type=boom"))
      TopicCommand.createTopic(zkClient, createOpts)
      fail("Expected exception on invalid topic-level config.")
    } catch {
      case _: Exception => // topic creation should fail due to the invalid config
    }
  }

}
