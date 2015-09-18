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
package unit.kafka.server

import java.util
import java.util.Properties

import junit.framework.Assert._
import kafka.admin.AdminUtils
import kafka.server.{ConfigType, KafkaConfig, KafkaServer, TopicCommandHelper}
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.admin.AlterTopicRequest.AlterTopicArguments
import org.apache.kafka.common.requests.admin.CreateTopicRequest.CreateTopicArguments
import org.apache.kafka.common.{ConfigEntry, PartitionReplicaAssignment}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnit3Suite

import scala.collection.JavaConverters._

class TopicCommandHelperTest extends ZooKeeperTestHarness {
  var servers: Seq[KafkaServer] = null

  @Before
  override def setUp() {
    super.setUp()

    servers = TestUtils.createBrokerConfigs(3, zkConnect, enableDeleteTopic = true)
      .map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
  }

  @After
  override def tearDown() {
    servers.foreach(server => CoreUtils.swallow(server.shutdown()))
    servers.foreach(server => CoreUtils.rm(server.config.logDirs))

    super.tearDown()
  }

  @Test
  def testCreateTopics(): Unit = {
    // partitions and replication-factor specified
    val topic1 = "t1"
    val createTopicCommand1 = new CreateTopicArguments(3, 2, new util.LinkedList[PartitionReplicaAssignment], new util.LinkedList[ConfigEntry]())

    // partitions and replication-factor, configs specified
    val topic2 = "t2"
    val createTopicCommand2 = new CreateTopicArguments(1, 1, new util.LinkedList[PartitionReplicaAssignment],
      util.Arrays.asList(new ConfigEntry("segment.bytes", "1000000"), new ConfigEntry("retention.bytes", "2000000")))

    // replica-assignment specified
    val topic3 = "t3"
    val partition0Assignment = new PartitionReplicaAssignment(0, util.Arrays.asList(0, 1, 2))
    val partition1Assignment = new PartitionReplicaAssignment(1, util.Arrays.asList(2, 1, 0))
    val partition2Assignment = new PartitionReplicaAssignment(2, util.Arrays.asList(2, 0, 1))
    val createTopicCommand3 = new CreateTopicArguments(-1, -1,
      util.Arrays.asList(partition0Assignment, partition1Assignment, partition2Assignment), new util.LinkedList[ConfigEntry]())

    val errors = TopicCommandHelper.createTopics(zkClient,
      Map(topic1 -> createTopicCommand1, topic2 -> createTopicCommand2, topic3 -> createTopicCommand3))

    // overall execution result
    assertEquals(Map(topic1 -> Errors.NONE, topic2 -> Errors.NONE, topic3 -> Errors.NONE), errors)

    val assignments = ZkUtils.getPartitionAssignmentForTopics(zkClient, Seq("t1", "t2", "t3"))
    val assignment_t1 = assignments("t1")
    val assignment_t2 = assignments("t2")
    val assignment_t3 = assignments("t3")

    // t1
    assertEquals(assignment_t1.size, createTopicCommand1.partitions)
    assertEquals(assignment_t1.values.head.size, createTopicCommand1.replicationFactor)

    // t2
    assertEquals(assignment_t2.size, createTopicCommand2.partitions)
    assertEquals(assignment_t2.values.head.size, createTopicCommand2.replicationFactor)

    val props = new Properties()
    createTopicCommand2.configs.asScala.foreach(entry =>
      props.put(entry.configKey(), entry.configValue())
    )
    assertEquals(props, AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, topic2))

    // t3
    val replicaAssignment_3 = createTopicCommand3.replicasAssignments.asScala.map {
      partitionReplicaAssignment =>
        (partitionReplicaAssignment.partition, partitionReplicaAssignment.replicas.asScala.map(_.intValue()))
    }.toMap
    assertEquals(assignment_t3, replicaAssignment_3)
  }

  @Test
  def testAlterTopics(): Unit = {
    trait TestCase {
      def topic: String

      def initialAssignment: Map[Int, Seq[Int]]

      def executePrecondition(): Unit = {
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, initialAssignment)
      }

      def alterationCommand: AlterTopicArguments

      def checkPostCondition(): Unit = {
        def actualAssignment() = {
          ZkUtils.getPartitionAssignmentForTopics(zkClient, Seq(topic))(topic)
        }

        if (alterationCommand.partitions != -1)
          TestUtils.waitUntilTrue(() => actualAssignment().size == alterationCommand.partitions,
            "Topic %s: partitions count doesn't match expected=%d".format(topic, alterationCommand.partitions))

        if (alterationCommand.replicationFactor != -1)
          TestUtils.waitUntilTrue(() => actualAssignment().head._2.size == alterationCommand.replicationFactor,
            "Topic %s: replication-factor doesn't match expected=%d".format(topic, alterationCommand.partitions))

        val targetAssignment = alterationCommand.replicasAssignments.asScala.map {
          partitionReplicaAssignment =>
            (partitionReplicaAssignment.partition, partitionReplicaAssignment.replicas.asScala.map(_.intValue()))
        }.toMap
        if (targetAssignment.nonEmpty)
          TestUtils.waitUntilTrue(() => actualAssignment() == initialAssignment ++ targetAssignment,
            "Topic %s: replica-assignment doesn't match expected=%s".format(topic, targetAssignment))
      }
    }
    // increase partitions
    val testCase1 = new TestCase {

      override def alterationCommand: AlterTopicArguments =
        new AlterTopicArguments(5, -1, new util.LinkedList[PartitionReplicaAssignment]())

      override def topic: String = "t1"

      override def initialAssignment: Map[Int, Seq[Int]] =
        Map(0 -> Seq(0, 1), 1 -> Seq(1, 2), 2 -> Seq(2, 0))
    }

    // increasing replication factor
    val testCase2 = new TestCase {

      override def alterationCommand: AlterTopicArguments =
        new AlterTopicArguments(-1, 2, new util.LinkedList[PartitionReplicaAssignment]())

      override def topic: String = "t2"

      override def initialAssignment: Map[Int, Seq[Int]] =
        Map(0 -> Seq(0), 1 -> Seq(1), 2 -> Seq(2))
    }

    // decreasing replication factor
    val testCase3 = new TestCase {

      override def alterationCommand: AlterTopicArguments =
        new AlterTopicArguments(-1, 1, new util.LinkedList[PartitionReplicaAssignment]())

      override def topic: String = "t3"

      override def initialAssignment: Map[Int, Seq[Int]] =
        Map(0 -> Seq(0, 1), 1 -> Seq(1, 2), 2 -> Seq(2, 0))
    }

    // replica-assignment - reassign-partitions
    val testCase4 = new TestCase {

      override def alterationCommand: AlterTopicArguments = {
        val t5_partition0Assignment = new PartitionReplicaAssignment(0, util.Arrays.asList(0, 1))
        val t5_partition1Assignment = new PartitionReplicaAssignment(1, util.Arrays.asList(2, 1))
        new AlterTopicArguments(-1, -1, util.Arrays.asList(t5_partition0Assignment, t5_partition1Assignment))
      }

      override def topic: String = "t4"

      override def initialAssignment: Map[Int, Seq[Int]] =
        Map(0 -> Seq(0, 1), 1 -> Seq(1, 2))
    }

    // replica-assignment - reassign-partitions increasing replication-factor

    val testCase5 = new TestCase {

      override def alterationCommand: AlterTopicArguments = {
        val t5_partition0Assignment = new PartitionReplicaAssignment(0, util.Arrays.asList(2, 1, 0))
        val t5_partition1Assignment = new PartitionReplicaAssignment(1, util.Arrays.asList(2, 1, 0))
        val t5_partition2Assignment = new PartitionReplicaAssignment(1, util.Arrays.asList(0, 1, 2))
        new AlterTopicArguments(-1, -1,
          util.Arrays.asList(t5_partition0Assignment, t5_partition1Assignment, t5_partition2Assignment))
      }

      override def topic: String = "t5"

      override def initialAssignment: Map[Int, Seq[Int]] =
        Map(0 -> Seq(0, 1), 1 -> Seq(1, 2))
    }

    // replica-assignment - increase partitions

    val testCase6 = new TestCase {

      override def alterationCommand: AlterTopicArguments = {
        val t6_partition2Assignment = new PartitionReplicaAssignment(2, util.Arrays.asList(2, 1))
        val t6_partition3Assignment = new PartitionReplicaAssignment(3, util.Arrays.asList(0, 1))
        new AlterTopicArguments(-1, -1,
          util.Arrays.asList(t6_partition2Assignment, t6_partition3Assignment))
      }

      override def topic: String = "t6"

      override def initialAssignment: Map[Int, Seq[Int]] =
        Map(0 -> Seq(0, 1), 1 -> Seq(1, 2))
    }

    // replica-assignment - just reassign some partitions

    val testCase7 = new TestCase {

      override def alterationCommand: AlterTopicArguments = {
        val t6_partition2Assignment = new PartitionReplicaAssignment(0, util.Arrays.asList(2, 1))
        new AlterTopicArguments(-1, -1,
          util.Arrays.asList(t6_partition2Assignment))
      }

      override def topic: String = "t7"

      override def initialAssignment: Map[Int, Seq[Int]] =
        Map(0 -> Seq(0, 1), 1 -> Seq(1, 2))
    }

    val testCases = Seq(testCase1, testCase2, testCase3, testCase4, testCase5, testCase6, testCase7)
    testCases.foreach(_.executePrecondition())


    val errors = TopicCommandHelper.alterTopics(zkClient, testCases.map(t => (t.topic, t.alterationCommand)).toMap)

    // overall execution result
    assertTrue(errors.size == testCases.size)
    assertTrue(errors.values.forall(_ == Errors.NONE))

    testCases.foreach(_.checkPostCondition())
  }

  @Test
  def testDeleteTopics(): Unit = {
    // topic eligible for deletion
    val topic1 = "t1"
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic1,
      Map(0 -> Seq(1, 2), 1 -> Seq(2, 1)))

    // topic that is in the deletion process
    val topic2 = "t2"
    AdminUtils.createTopic(zkClient, topic2, 1, servers.size)
    // shutdown one of the servers so topic with replication-factor = 3
    // will be ineligible for deletion and will stay in zk admin path
    servers.find(s => s.config.brokerId == 0).foreach(_.shutdown())
    TestUtils.waitUntilTrue(() => ZkUtils.getAllBrokersInCluster(zkClient).size == 2, "Server can't shutdown")
    AdminUtils.deleteTopic(zkClient, topic2)
    assertTrue(ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic2)))

    // just another topic to check topic doesn't exist case
    val topic3 = "t3"
    val wrongTopic3 = "0t3"
    AdminUtils.createTopic(zkClient, topic3, 1, 2)

    val errors = TopicCommandHelper.deleteTopics(zkClient, Set(topic1, topic2, wrongTopic3))

    // overall execution result
    assertEquals(Map(topic1 -> Errors.NONE, topic2 -> Errors.NONE, wrongTopic3 -> Errors.INVALID_TOPIC_EXCEPTION), errors)

    TestUtils.waitUntilTrue(() => AdminUtils.topicExists(zkClient, topic1), "Topic " + topic1 + " is not deleted")
    assertTrue(AdminUtils.topicExists(zkClient, topic2))
    assertTrue(ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic2)))
  }

}
