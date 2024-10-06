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

import kafka.log.UnifiedLog
import kafka.server.{KafkaBroker, KafkaConfig, QuorumTestHarness}
import kafka.utils.TestUtils.waitForAllPartitionsMetadata
import kafka.utils._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewPartitionReassignment, NewPartitions}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{TopicDeletionDisabledException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.concurrent.ExecutionException
import java.util.{Collections, Optional, Properties}
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._
import scala.util.Using

class DeleteTopicTest extends QuorumTestHarness {

  var brokers: Seq[KafkaBroker] = Seq()

  var admin: Admin = _

  val topic = "test"

  case class Assignment(
    replicas: Seq[Int],
    addingReplicas: Seq[Int],
    removingReplicas: Seq[Int])

  val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
  val expectedReplicaFullAssignment = expectedReplicaAssignment.map { case (k, v) =>
    k -> Assignment(v, List(), List())
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(brokers)
    if (admin != null) admin.close()
    super.tearDown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteTopicWithAllAliveReplicas(quorum: String): Unit = {
    brokers = createTestTopicAndCluster(topic)
    // start topic deletion
    admin.deleteTopics(Collections.singletonList(topic)).all().get()
    TestUtils.verifyTopicDeletion(null, topic, 1, brokers)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testResumeDeleteTopicWithRecoveredFollower(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    brokers = createTestTopicAndCluster(topic)
    // shut down one follower replica
    val leaderId = TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic, 0))
    val follower = brokers.filter(s => s.config.brokerId != leaderId).last
    follower.shutdown()
    // start topic deletion
    admin.deleteTopics(Collections.singletonList(topic)).all().get()
    // check if all replicas but the one that is shut down has deleted the log
    TestUtils.waitUntilTrue(() =>
      brokers.filter(s => s.config.brokerId != follower.config.brokerId)
        .forall(_.logManager.getLog(topicPartition).isEmpty), "Replicas 0,1 have not deleted log.")

    // restart follower replica
    follower.startup()
    TestUtils.verifyTopicDeletion(null, topic, 1, brokers)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testPartitionReassignmentDuringDeleteTopic(quorum: String): Unit = {
    val topicPartition = new TopicPartition(topic, 0)

    // create brokers
    brokers = createTestTopicAndCluster(topic, 4)
    val servers = brokers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))

    val leaderIdOpt = TestUtils.waitUntilLeaderIsKnown(brokers, topicPartition)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt).last
    follower.shutdown()
    // start topic deletion
    admin.deleteTopics(Collections.singletonList(topic)).all().get()
    // verify that a partition from the topic cannot be reassigned
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.plaintextBootstrapServers(servers))
    val adminClient = Admin.create(props)
    try {
      waitUntilTopicGone(adminClient, "test")
      verifyReassignmentFailsForMissing(adminClient, new TopicPartition(topic, 0),
        new NewPartitionReassignment(util.Arrays.asList(1, 2, 3)))
    } finally {
      adminClient.close()
    }
    follower.startup()
    TestUtils.verifyTopicDeletion(null, topic, 1, servers)
  }

  private def waitUntilTopicGone(adminClient: Admin, topicName: String): Unit = {
    TestUtils.waitUntilTrue(() => {
      try {
        adminClient.describeTopics(util.Collections.singletonList(topicName)).allTopicNames().get()
        false
      } catch {
        case e: ExecutionException =>
          classOf[UnknownTopicOrPartitionException].equals(e.getCause.getClass)
      }
    }, s"Topic ${topicName} should be deleted.")
  }

  private def verifyReassignmentFailsForMissing(adminClient: Admin,
                                                partition: TopicPartition,
                                                reassignment: NewPartitionReassignment): Unit = {
    val e = assertThrows(classOf[ExecutionException], () => adminClient.alterPartitionReassignments(Collections.singletonMap(partition,
      Optional.of(reassignment))).all().get())
    assertEquals(classOf[UnknownTopicOrPartitionException], e.getCause.getClass)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncreasePartitionCountDuringDeleteTopic(quorum: String): Unit = {
    val topicPartition = new TopicPartition(topic, 0)
    val allBrokers = createTestTopicAndCluster(topic, 4)
    this.brokers = allBrokers
    val partitionHostingBrokers = allBrokers.filter(b => expectedReplicaAssignment(0).contains(b.config.brokerId))

    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => partitionHostingBrokers.forall(_.logManager.getLog(topicPartition).isDefined),
      "Replicas for topic test not created.")

    val leaderIdOpt = TestUtils.waitUntilLeaderIsKnown(partitionHostingBrokers, topicPartition)

    // shutdown a broker to make sure the following topic deletion will be suspended
    val follower = partitionHostingBrokers.filter(s => s.config.brokerId != leaderIdOpt).last
    follower.shutdown()
    // start topic deletion
    admin.deleteTopics(Collections.singletonList(topic)).all().get()

    // increase the partition count for topic
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.plaintextBootstrapServers(partitionHostingBrokers))
    Using(Admin.create(props)) { adminClient =>
      try {
        adminClient.createPartitions(Map(topic -> NewPartitions.increaseTo(2)).asJava).all().get()
      } catch {
        case _: ExecutionException =>
      }
    }

    // bring back the failed broker
    follower.startup()

    TestUtils.verifyTopicDeletion(null, topic, 2, partitionHostingBrokers)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteTopicDuringAddPartition(quorum: String): Unit = {
    brokers = createTestTopicAndCluster(topic)
    val leaderIdOpt = TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic, 0))
    val follower = brokers.filter(_.config.brokerId != leaderIdOpt).last
    val newPartition = new TopicPartition(topic, 1)

    follower.shutdown()
    // wait until the broker is in shutting down state
    TestUtils.waitUntilTrue(() => follower.brokerState == BrokerState.SHUTTING_DOWN,
      s"Follower ${follower.config.brokerId} was not shut down")

    increasePartitions(admin, topic, 3, brokers.filter(_.config.brokerId != follower.config.brokerId))

    // start topic deletion
    admin.deleteTopics(Collections.singletonList(topic)).all().get()
    follower.startup()
    // test if topic deletion is resumed
    TestUtils.verifyTopicDeletion(null, topic, 1, brokers)
    // verify that new partition doesn't exist on any broker either
    TestUtils.waitUntilTrue(() =>
      brokers.forall(_.logManager.getLog(newPartition).isEmpty),
      "Replica logs not for new partition [test,1] not deleted after delete topic is complete.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAddPartitionDuringDeleteTopic(quorum: String): Unit = {
    brokers = createTestTopicAndCluster(topic)
    // partitions to be added to the topic later
    val newPartition = new TopicPartition(topic, 1)

    admin.deleteTopics(Collections.singletonList(topic)).all().get()
    // pass empty list of brokers to avoid validating metadata since the topic is being deleted.
    increasePartitions(admin, topic, 3, Seq.empty)

    TestUtils.verifyTopicDeletion(null, topic, 1, brokers)
    // verify that new partition doesn't exist on any broker either
    assertTrue(brokers.forall(_.logManager.getLog(newPartition).isEmpty), "Replica logs not deleted after delete topic is complete")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testRecreateTopicAfterDeletion(quorum: String): Unit = {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topicPartition = new TopicPartition(topic, 0)
    brokers = createTestTopicAndCluster(topic)
    // start topic deletion
    admin.deleteTopics(Collections.singletonList(topic)).all().get()
    TestUtils.verifyTopicDeletion(null, topic, 1, brokers)
    // re-create topic on same replicas
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment = expectedReplicaAssignment)
    // check if all replica logs are created
    TestUtils.waitUntilTrue(() => brokers.forall(_.logManager.getLog(topicPartition).isDefined),
      "Replicas for topic test not created.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteNonExistingTopic(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    brokers = createTestTopicAndCluster(topic)

    // start topic deletion
    TestUtils.waitUntilTrue(() => {
      try {
        admin.deleteTopics(Collections.singletonList("test2")).all().get()
        false
      } catch {
        case e: ExecutionException =>
          classOf[UnknownTopicOrPartitionException].equals(e.getCause.getClass)
      }
    }, s"Topic test2 should not exist.")

    // verify delete topic path for test2 is removed from ZooKeeper
    TestUtils.verifyTopicDeletion(null, "test2", 1, brokers)
    // verify that topic test is untouched
    TestUtils.waitUntilTrue(() => brokers.forall(_.logManager.getLog(topicPartition).isDefined),
      "Replicas for topic test not created")

    TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(admin, topic, 0, 1000)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDeleteTopicWithCleaner(quorum: String): Unit = {
    val topicName = "test"
    val topicPartition = new TopicPartition(topicName, 0)
    val topic = topicPartition.topic

    val brokerConfigs = TestUtils.createBrokerConfigs(3, null, false)
    brokerConfigs.head.setProperty("delete.topic.enable", "true")
    brokerConfigs.head.setProperty("log.cleaner.enable","true")
    brokerConfigs.head.setProperty("log.cleanup.policy","compact")
    brokerConfigs.head.setProperty("log.segment.bytes","100")
    brokerConfigs.head.setProperty("log.cleaner.dedupe.buffer.size","1048577")

    brokers = createTestTopicAndCluster(topic, brokerConfigs, expectedReplicaAssignment)

    // for simplicity, we are validating cleaner offsets on a single broker
    val server = brokers.head
    val log = server.logManager.getLog(topicPartition).get

    // write to the topic to activate cleaner
    writeDups(numKeys = 100, numDups = 3,log)

    // wait for cleaner to clean
   server.logManager.cleaner.awaitCleaned(new TopicPartition(topicName, 0), 0)

    // delete topic
    admin.deleteTopics(Collections.singletonList(topic)).all().get()
    TestUtils.verifyTopicDeletion(null, "test", 1, brokers)
  }

  private def createTestTopicAndCluster(topic: String, numOfConfigs: Int = 3, deleteTopicEnabled: Boolean = true, replicaAssignment: Map[Int, List[Int]] = expectedReplicaAssignment): Seq[KafkaBroker] = {
    val brokerConfigs = TestUtils.createBrokerConfigs(numOfConfigs, null, enableControlledShutdown = false)
    brokerConfigs.foreach(_.setProperty("delete.topic.enable", deleteTopicEnabled.toString))
    createTestTopicAndCluster(topic, brokerConfigs, replicaAssignment)
  }

  private def createTestTopicAndCluster(topic: String, brokerConfigs: Seq[Properties], replicaAssignment: Map[Int, List[Int]]): Seq[KafkaBroker] = {
    val topicPartition = new TopicPartition(topic, 0)
    // create brokers
    val brokers = brokerConfigs.map(b => createBroker(KafkaConfig.fromProps(b)))

    admin = TestUtils.createAdminClient(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    TestUtils.createTopicWithAdmin(admin, topic, brokers, controllerServers, replicaAssignment = replicaAssignment)

    val brokersHostingTopicPartition = brokers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => brokersHostingTopicPartition.forall(_.logManager.getLog(topicPartition).isDefined),
    "Replicas for topic test not created")

    brokers
  }

  private def writeDups(numKeys: Int, numDups: Int, log: UnifiedLog): Seq[(Int, Int)] = {
    var counter = 0
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.appendAsLeader(TestUtils.singletonRecords(value = counter.toString.getBytes, key = key.toString.getBytes), leaderEpoch = 0)
      counter += 1
      (key, count)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testDisableDeleteTopic(quorum: String): Unit = {
    val topicPartition = new TopicPartition(topic, 0)

    // Restart KRaft quorum with the updated config
    val overridingProps = new Properties()
    overridingProps.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, false.toString)
    if (implementation != null)
      implementation.shutdown()
    implementation = newKRaftQuorum(overridingProps)

    brokers = createTestTopicAndCluster(topic, deleteTopicEnabled = false)
    TestUtils.waitUntilTrue(() => {
      try {
        admin.deleteTopics(Collections.singletonList(topic)).all().get()
        false
      } catch {
        case e: ExecutionException =>
          classOf[TopicDeletionDisabledException].equals(e.getCause.getClass)
      }
    }, s"TopicDeletionDisabledException should be returned when deleting ${topic}.")

    // verify that topic test is untouched
    assertTrue(brokers.forall(_.logManager.getLog(topicPartition).isDefined))
    assertDoesNotThrow(() => TestUtils.describeTopic(admin, topic))
    // topic test should have a leader
    TestUtils.waitUntilLeaderIsKnown(brokers, topicPartition)
  }

  private def increasePartitions[B <: KafkaBroker](admin: Admin,
                                           topic: String,
                                           totalPartitionCount: Int,
                                           brokersToValidate: Seq[B]
                                          ): Unit = {
    val newPartitionSet: Map[String, NewPartitions] = Map.apply(topic -> NewPartitions.increaseTo(totalPartitionCount))
    admin.createPartitions(newPartitionSet.asJava)

    if (brokersToValidate.nonEmpty) {
      // wait until we've propagated all partitions metadata to all brokers
      val allPartitionsMetadata = waitForAllPartitionsMetadata(brokersToValidate, topic, totalPartitionCount)
      (0 until totalPartitionCount - 1).foreach(i => {
        allPartitionsMetadata.get(new TopicPartition(topic, i)).foreach { partitionMetadata =>
          assertEquals(totalPartitionCount, partitionMetadata.replicas.size)
        }
      })
    }
  }
}
