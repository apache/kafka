/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.admin

import java.util.Optional

import kafka.admin.TopicCommand.ZookeeperTopicService
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{AdminClientConfig, NewPartitionReassignment, NewTopic, AdminClient => JAdminClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.Seq

class ReassignPartitionsIntegrationTest extends ZooKeeperTestHarness with RackAwareTest {
  import ReassignPartitionsIntegrationTest._

  var servers: Seq[KafkaServer] = Seq()
  val broker1 = 0
  val broker2 = 1
  val broker3 = 2
  val broker4 = 3
  val broker5 = 4
  val broker6 = 5
  val rack = Map(
    broker1 -> "rack1",
    broker2 -> "rack2",
    broker3 -> "rack2",
    broker4 -> "rack1",
    broker5 -> "rack3",
    broker6 -> "rack3"
  )

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val brokerConfigs = TestUtils.createBrokerConfigs(6, zkConnect, enableControlledShutdown = true)
    servers = brokerConfigs.map { config =>
      config.setProperty(KafkaConfig.RackProp, rack(config.getProperty(KafkaConfig.BrokerIdProp).toInt))
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      config.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp, "1")
      config.setProperty(KafkaConfig.ControlledShutdownRetryBackoffMsProp, "1000")
      config.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp, "1000")
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testRackAwareReassign(): Unit = {
    val numPartitions = 18
    val replicationFactor = 3

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--disable-rack-aware",
      "--topic", "foo"))
    new ZookeeperTopicService(zkClient).createTopic(createOpts)

    val topicJson = """{"topics": [{"topic": "foo"}], "version":1}"""
    val (proposedAssignment, currentAssignment) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rack.keys.toSeq.sorted, topicJson, disableRackAware = false)

    val assignment = proposedAssignment map { case (topicPartition, replicas) =>
      (topicPartition.partition, replicas)
    }
    checkReplicaDistribution(assignment, rack, rack.size, numPartitions, replicationFactor)
  }

  @Test
  def testReassignPartition(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "test-topic"
      val partition = 0: Integer

      val partitionAssignment = Map(partition -> Seq(broker1: Integer, broker2:Integer).asJava).asJava
      val newTopic = new NewTopic(topic, partitionAssignment)
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))

      // Reassign replicas to different brokers
      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker3, broker4))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker3, broker4))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker3, broker4))
    }
  }

  @Test
  def testInvalidReplicaIds(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "test-topic"
      val partition = 0: Integer

      val partitionAssignment = Map(partition -> Seq(broker1: Integer, broker2: Integer).asJava).asJava
      val newTopic = new NewTopic(topic, partitionAssignment)
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))

      // Test reassignment with duplicate broker ids
      var future = client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker4, broker5, broker5))).asJava
      ).all()
      JTestUtils.assertFutureThrows(future, classOf[InvalidReplicaAssignmentException])

      // Test reassignment with invalid broker ids
      future = client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(-1, broker3))).asJava
      ).all()
      JTestUtils.assertFutureThrows(future, classOf[InvalidReplicaAssignmentException])

      // Test reassignment with extra broker ids
      future = client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(6, broker2, broker3))).asJava
      ).all()
      JTestUtils.assertFutureThrows(future, classOf[InvalidReplicaAssignmentException])
    }
  }
}

object ReassignPartitionsIntegrationTest {
  def createConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT")),
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "20000"
    )
  }

  def reassignmentEntry(replicas: Seq[Int]): Optional[NewPartitionReassignment] = {
    Optional.of(new NewPartitionReassignment(replicas.map(r => r: Integer).asJava))
  }

  def waitForAllReassignmentsToComplete(client: JAdminClient): Unit = {
    TestUtils.waitUntilTrue(() => client.listPartitionReassignments().reassignments().get().isEmpty,
      s"There still are ongoing reassignments", pause = 100L)
  }
}
