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

import kafka.common.TopicExistsException
import org.junit.Assert._
import org.junit.Test
import kafka.utils.Logging
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import kafka.server.ConfigType
import kafka.admin.TopicCommand.TopicCommandOptions
import kafka.utils.ZkUtils._
import kafka.coordinator.GroupCoordinator
import org.apache.kafka.common.internals.TopicConstants

class TopicCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  @Test
  def testConfigPreservationAcrossPartitionAlteration() {
    val topic = "test"
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkUtils, brokers)
    // create the topic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", topic))
    TopicCommand.createTopic(zkUtils, createOpts)
    val props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    zkUtils.createPersistentPath(EntityConfigChangesPath)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString, "--topic", topic))
    TopicCommand.alterTopic(zkUtils, alterOpts)
    val newProps = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
  }

  @Test
  def testTopicDeletion() {
    val normalTopic = "test"

    val numPartitionsOriginal = 1

    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkUtils, brokers)

    // create the NormalTopic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", normalTopic))
    TopicCommand.createTopic(zkUtils, createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", normalTopic))
    val deletePath = getDeleteTopicPath(normalTopic)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkUtils.zkClient.exists(deletePath))
    TopicCommand.deleteTopic(zkUtils, deleteOpts)
    assertTrue("Delete path for topic should exist after deletion.", zkUtils.zkClient.exists(deletePath))

    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", TopicConstants.GROUP_METADATA_TOPIC_NAME))
    TopicCommand.createTopic(zkUtils, createOffsetTopicOpts)

    // try to delete the TopicConstants.GROUP_METADATA_TOPIC_NAME and make sure it doesn't
    val deleteOffsetTopicOpts = new TopicCommandOptions(Array("--topic", TopicConstants.GROUP_METADATA_TOPIC_NAME))
    val deleteOffsetTopicPath = getDeleteTopicPath(TopicConstants.GROUP_METADATA_TOPIC_NAME)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkUtils.zkClient.exists(deleteOffsetTopicPath))
    intercept[AdminOperationException] {
        TopicCommand.deleteTopic(zkUtils, deleteOffsetTopicOpts)
    }
    assertFalse("Delete path for topic shouldn't exist after deletion.", zkUtils.zkClient.exists(deleteOffsetTopicPath))
  }

  @Test
  def testDeleteIfExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkUtils, brokers)

    // delete a topic that does not exist without --if-exists
    val deleteOpts = new TopicCommandOptions(Array("--topic", "test"))
    intercept[IllegalArgumentException] {
      TopicCommand.deleteTopic(zkUtils, deleteOpts)
    }

    // delete a topic that does not exist with --if-exists
    val deleteExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--if-exists"))
    TopicCommand.deleteTopic(zkUtils, deleteExistsOpts)
  }

  @Test
  def testAlterIfExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkUtils, brokers)

    // alter a topic that does not exist without --if-exists
    val alterOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1"))
    intercept[IllegalArgumentException] {
      TopicCommand.alterTopic(zkUtils, alterOpts)
    }

    // alter a topic that does not exist with --if-exists
    val alterExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1", "--if-exists"))
    TopicCommand.alterTopic(zkUtils, alterExistsOpts)
  }

  @Test
  def testCreateIfNotExists() {
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkUtils, brokers)

    val topic = "test"
    val numPartitions = 1

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", topic))
    TopicCommand.createTopic(zkUtils, createOpts)

    // try to re-create the topic without --if-not-exists
    intercept[TopicExistsException] {
      TopicCommand.createTopic(zkUtils, createOpts)
    }

    // try to re-create the topic with --if-not-exists
    val createNotExistsOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", topic, "--if-not-exists"))
    TopicCommand.createTopic(zkUtils, createNotExistsOpts)
  }

  @Test
  def testCreateAlterTopicWithRackAware() {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkUtils)

    val numPartitions = 18
    val replicationFactor = 3
    val createOpts = new TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--topic", "foo"))
    TopicCommand.createTopic(zkUtils, createOpts)

    var assignment = zkUtils.getReplicaAssignmentForTopics(Seq("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)

    val alteredNumPartitions = 36
    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", alteredNumPartitions.toString,
      "--topic", "foo"))
    TopicCommand.alterTopic(zkUtils, alterOpts)
    assignment = zkUtils.getReplicaAssignmentForTopics(Seq("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor)
  }
}
